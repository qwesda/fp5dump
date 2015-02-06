import os
import sys
import logging
import struct
import codecs
import logging
import time

from array import array
from binascii import hexlify, unhexlify

import psycopg2

from fp5dump.fp5file.block import TokenType, Token, decode_vli
from fp5dump.fp5file.blockchain import BlockChain, BlockChainIter
from fp5dump.fp5file.datafield import DataField

class FP5File(object):
    """Wrapper for FP5 file object"""

    def __init__(self, filename, encoding='latin1'):
        super(FP5File, self).__init__()

        self.logging = logging.getLogger('fp5dump.fp5file.fp5file')

        self.filename = filename

        self.encoding = encoding
        self.basename = os.path.basename(filename)
        self.db_name = os.path.basename(filename)

        self.block_chains = []
        self.block_chain_levels = 0
        self.fields = {}

        self.index = None
        self.data = None

        self.records_index = []
        self.records_count = 0
        self.exported_records = 0

        self.block_prev_id_to_block_pos = None
        self.block_id_to_block_pos = None

        self.file = open(self.filename, "rb", buffering=0)

        self.largest_block_id = 0x00000000

        self.get_blocks()
        self.order_block_indices()
        self.get_field_index()
        self.get_record_index()

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()

    def close(self):
        self.logging.info("closing %s" % self.basename)

        if self.file:
            self.file.close()

    def get_blocks(self):
        self.logging.info("opening %s" % self.basename)

        file_size = os.path.getsize(self.filename)

        if file_size % 0x400 != 0:
            raise Exception("File size is not a multiple of 0x400")

        try:
            pos = 0x800

            while pos < file_size:
                self.file.seek(pos)

                (deleted_flag, level, prev_id, next_id) = struct.unpack_from(">BBII", self.file.read(10))

                if pos == 0x800:
                    self.largest_block_id = next_id
                    self.block_prev_id_to_block_pos = array('I', b'\x00\x00\x00\x00' * (self.largest_block_id + 1))
                    self.block_id_to_block_pos = array('I', b'\x00\x00\x00\x00' * (self.largest_block_id + 1))

                    self.block_chain_levels = level

                    for i in range(0, self.block_chain_levels + 1):
                        self.block_chains.append(BlockChain(self, i))

                    self.index = self.block_chains[level]
                    self.index.first_block_pos = pos

                    self.data = self.block_chains[0]

                if deleted_flag != 0xff:
                    if prev_id == 0x00000000:
                        self.block_chains[level].first_block_pos = pos
                    else:
                        if self.block_prev_id_to_block_pos[prev_id] == 0x00000000:
                            self.block_prev_id_to_block_pos[prev_id] = pos
                        else:
                            print("block with duplicate prev_id 0x%08X found for level %d" % (prev_id, level))

                pos += 0x400

        except Exception as e:
            raise e

    def order_block_indices(self):
        for level in reversed(range(0, self.block_chain_levels + 1)):
            self.logging.debug("ordering index %d" % level)

            block_chain = self.block_chains[level]

            if level > 0:
                block_chain.daughter_block_chain = self.block_chains[level - 1]

            if level < self.block_chain_levels:
                block_chain.parent_block_chain = self.block_chains[level + 1]

            block_chain.order_blocks()

    def dump_index_blocks(self, output_filename=None):
        self.logging.info("dump index blocks")

        if not output_filename:
            output_filename = self.filename + ".index"

        with open(output_filename, "wb") as file:
            for level in reversed(range(0, self.block_chain_levels + 1)):
                if level > 0:
                    block_chain = self.block_chains[level]

                    for block in block_chain:
                        file.write(block.get_block_bytes_from_file(self.file, True))

    def dump_data_blocks(self, output_filename=None):
        self.logging.info("dump data blocks")

        if not output_filename:
            output_filename = self.filename + ".index"

        with open(output_filename, "wb") as file:
            for block in self.data:
                file.write(block.get_block_bytes_from_file(self.file, True))

    def dump_blocks_with_path(self, search_path, output_filename=None):
        self.logging.info("dump data with path %r" % search_path)

        if not output_filename:
            output_filename = self.filename + "." + search_path.decode().replace("/", "_") + ".data"

        with open(output_filename, "wb") as file:
            start_block = self.index.find_first_block_id_for_path(search_path)

            for block in BlockChainIter(self.data, start_block):
                first_token_path_block = block.get_first_token_path(self.file)

                if first_token_path_block > search_path and not first_token_path_block.startswith(search_path):
                    break
                else:
                    file.write(block.get_block_bytes_from_file(self.file, True))

    def get_data_with_path(self, search_path):
        if type(search_path) is list:
            search_path = b'/'.join(hexlify(part) for part in search_path)
        elif type(search_path) is bytes:
            search_path = search_path.lower()

        tokens = []

        for token in self.data.tokens(search_path):
            tokens.append(token)

        return tokens

    def get_sub_data_with_path(self, search_path, first_sub_record_to_export=None):
        if type(search_path) is list:
            search_path = b'/'.join(hexlify(part) for part in search_path)
        elif type(search_path) is bytes:
            search_path = search_path.lower()

        tokens = []

        search_path_split_len = len(search_path.split(b'/'))
        last_sub_data_key = None
        last_sub_data_start = None

        if first_sub_record_to_export is not None:
            first_sub_record_to_export = hexlify(first_sub_record_to_export)
            start_block = self.index.find_first_block_id_for_path(b'/'.join([search_path, first_sub_record_to_export]))
        else:
            start_block = None

        for token in self.data.tokens(search_path, start_block=start_block):
            token_path_split = token.path.split(b'/')

            if len(token_path_split) > search_path_split_len:
                sub_data_key = token_path_split[search_path_split_len]
            else:
                sub_data_key = None

            if first_sub_record_to_export is not None and first_sub_record_to_export > sub_data_key:
                continue

            if sub_data_key != last_sub_data_key:
                if last_sub_data_key is not None and last_sub_data_start < len(tokens):
                    yield tokens[last_sub_data_start:]

                    del tokens[last_sub_data_start:]

                if sub_data_key is not None:
                    last_sub_data_start = len(tokens)

                last_sub_data_key = sub_data_key

            tokens.append(token)

        if last_sub_data_key is not None and last_sub_data_start < len(tokens):
            yield tokens[last_sub_data_start:]

            del tokens[last_sub_data_start:]

    def get_field_index(self):
        self.logging.debug("get_field_index")

        self.fields = {}

        for id_token in self.get_data_with_path(b'03/01'):
            field_id = decode_vli(id_token.data[1:])

            if field_id in self.fields:
                print("duplicate id for field", id_token)

            self.fields[field_id] = DataField(field_id, id_token.field_ref_bin)

        for type_token in self.get_data_with_path(b'03/02'):
            field_type = type_token.path[-1] - 0x30

            for field_id in type_token.data:
                field_id = decode_vli(field_id)

                if field_id in self.fields:
                    self.fields[field_id].type = field_type
                else:
                    print("unhandled field id in field type index", field_id)

        for field_type_nr in self.get_data_with_path(b'03/03'):
            field_id = decode_vli(field_type_nr.data[1:])

            if field_id in self.fields:
                field_nr = int.from_bytes(field_type_nr.field_ref_bin, byteorder='big')

                self.fields[field_id].order = field_nr
            else:
                print("unhandled field id in field type index", field_id)

        for field_option_tokens in self.get_sub_data_with_path(b'03/05'):
            field_id = decode_vli(unhexlify(field_option_tokens[0].path.split(b'/')[2]))

            if field_id in self.fields:
                field = self.fields[field_id]

                for option_token in field_option_tokens:

                    token_sub_path_split = option_token.path.split(b'/')[3:]

                    if len(token_sub_path_split) == 0:
                        if option_token.field_ref == 1:
                            field.label = option_token.data.decode(self.encoding)

                        if option_token.field_ref == 2:
                            field.stored = option_token.data[0] <= 0x02
                            field.indexed = option_token.data[2] == 0x01

                            field.repetitions = option_token.data[11]
            else:
                print("unhandled field id in field type index", field_id)

    def get_record_index(self):
        self.logging.debug("get_record_index")

        tokens = self.get_data_with_path(b'0D')

        if len(tokens) == 1 and tokens[0].type == TokenType.x8N:
            self.records_index = tokens[0].data
            self.records_count = len(self.records_index)

    def insert_records_into_postgres(self, field_ids_to_dump, first_record_to_export=None, table_name=None, psycopg2_connect_string=None, schema=None):
        self.logging.info("inserting records")

        def create_table():
            pgsql_fields = []

            for field_id_to_export in field_ids_to_dump:
                field_to_export = self.fields[field_id_to_export]

                if field_to_export.repetitions == 1:
                    pgsql_fields.append('    "%s" text' % field_to_export.label)
                else:
                    pgsql_fields.append('    "%s" text[]' % field_to_export.label)

            cursor.execute('DROP TABLE IF EXISTS "%s";' % table_name)
            cursor.execute('CREATE TABLE IF NOT EXISTS "%s" (\n%s\n);\n\n' % (table_name, ',\n'.join(pgsql_fields)))

            conn.commit()


        conn = psycopg2.connect(psycopg2_connect_string)
        cursor = conn.cursor()

        if table_name is None:
            table_name = self.db_name

        if schema is not None:
            cursor.execute("SET SCHEMA '%s';" % schema)

        create_table()

        for record_tokens in self.get_sub_data_with_path(b'05', first_sub_record_to_export=first_record_to_export):
            # record_id = unhexlify(record_tokens[0].path.split(b'/')[1])
            record_path = b'/'.join(record_tokens[0].path.split(b'/')[:2])

            values = []
            fields_present = []

            for record_token in record_tokens:
                if record_path == record_token.path:
                    field_ref = record_token.field_ref
                elif len(record_token.path.split(b'/')) == 3:
                    field_ref = decode_vli(unhexlify(b'/'.join(record_token.path.split(b'/')[2:])))
                else:
                    field_ref = None

                if field_ref in field_ids_to_dump:
                    field = self.fields[field_ref]

                    if field.repetitions <= 1:
                        fields_present.append(field_ref)
                        values.append(record_token.data.decode(self.encoding))
                    elif field_ref in fields_present:
                        values[fields_present.index(field_ref)][record_token.field_sub_ref-1] = record_token.data.decode(self.encoding)
                    else:
                        fields_present.append(field_ref)
                        values.append([None] * field.repetitions)
                        values[fields_present.index(field_ref)][record_token.field_sub_ref-1] = record_token.data.decode(self.encoding)

            if len(fields_present):
                fields = list('"' + self.fields[field_id].label + '"' for field_id in fields_present)

                statement = 'INSERT INTO "%s" (%s) VALUES (%s)' % (table_name, ", ".join(fields), ("%s, "*len(fields_present))[:-2] )

                cursor.execute(statement, values)

                self.exported_records += 1

            if self.exported_records % 1000 == 0:
                conn.commit()

        # self.logging.info("exported %d records from '%s'" % (self.exported_records, self.filename))

        conn.close()

        # print("exported %d records" % self.exported_records)

    def dump_records_pgsql(self, field_ids_to_dump, first_record_to_export=None, filename=None, table_name=None, show_progress=False):
        self.logging.info("dumping records")

        def write_batch():
            fields_in_batch = [self.fields[field_id] for field_id in field_ids_to_dump if field_id in batch_used_columns]
            field_ids_in_batch = [field_id for field_id in field_ids_to_dump if field_id in batch_used_columns]

            fields_not_in_table = [field for field in fields_in_batch if field.id not in table_used_columns]

            if len(table_used_columns) == 0:
                output.write('DROP TABLE IF EXISTS "%s";\nCREATE TABLE IF NOT EXISTS "%s" (%s);' % (table_name, table_name,
                    ", ".join(['\n\t"%s" %s' % (field.label, "text" if field.repetitions <= 1 else "text[]")
                        for field in fields_not_in_table ])))

            elif len(fields_not_in_table) > 0:
                output.write('\n\nALTER TABLE "%s" %s;' % (table_name,
                    ", ".join(['\n\tADD COLUMN "%s" %s' % (field.label, "text" if field.repetitions <= 1 else "text[]")
                        for field in fields_not_in_table ])))

            output.write('\n\nINSERT INTO "%s" (\n\t' % table_name)
            output.write( ', \n\t'.join('"%s"' % field.label for field in fields_in_batch ) )
            output.write('\n) VALUES (')

            batch_count = len(batch)
            batch_counter = 0

            for (fields_present, data_tokens) in batch:
                for field in fields_in_batch:
                    is_last_data_token = field is fields_in_batch[-1]

                    if field.id in fields_present:
                        data_token = data_tokens[fields_present.index(field.id)]

                        if field.repetitions <= 1:
                            output.write("\n\tE'")
                            output.write(data_token.data.decode(self.encoding).translate(trans))
                            output.write("'," if not is_last_data_token else "'")
                        else:
                            output.write('\n\tARRAY[')

                            for i, sub_data in enumerate(data_token.data):
                                if sub_data:
                                    output.write("E'")
                                    output.write(sub_data.decode(self.encoding).translate(trans))
                                    output.write("'")
                                else:
                                    output.write("NULL")

                                output.write(", " if i != field.repetitions-1 else '')

                            output.write('],' if not is_last_data_token else ']')
                    else:
                        if field.repetitions <= 1:
                            output.write('\n\tNULL,' if not is_last_data_token else '\n\tNULL')
                        else:
                            output.write('\n\tARRAY[')
                            output.write(", ".join(["NULL"] * field.repetitions))
                            output.write('],' if not is_last_data_token else ']')

                batch_counter += 1

                output.write('\n), (' if batch_counter != batch_count else '\n);')

            table_used_columns.update(batch_used_columns)
            batch_used_columns.clear()
            batch.clear()

        if table_name is None:
            table_name = self.db_name

        if filename is None:
            filename = self.basename + '.psql'

        output = codecs.open(filename, 'wb', encoding="utf8", buffering=0x800000)

        start_time = time.time()
        eta_last_updated = start_time

        trans = "".maketrans({
            '\\': '\\\\',
            '\'': '\\\'',
            '\b': '\\b',
            '\f': '\\f',
            '\n': '\\n',
            '\r': '\\r',
            '\t': '\\t',
            '\x00': ''
        })

        batch = []
        table_used_columns = set()
        batch_used_columns = set()

        for record_tokens in self.get_sub_data_with_path(b'05', first_sub_record_to_export=first_record_to_export):
            record_id = unhexlify(record_tokens[0].path.split(b'/')[1])
            record_path = b'/'.join(record_tokens[0].path.split(b'/')[:2])

            data_tokens = []
            fields_present = []

            for record_token in record_tokens:
                if record_path == record_token.path:
                    field_ref = record_token.field_ref
                elif len(record_token.path.split(b'/')) == 3:
                    field_ref = decode_vli(unhexlify(b'/'.join(record_token.path.split(b'/')[2:])))
                else:
                    field_ref = None

                if field_ref in field_ids_to_dump:
                    field = self.fields[field_ref]

                    if field.repetitions <= 1:
                        fields_present.append(field_ref)
                        data_tokens.append(record_token)
                    elif field_ref in fields_present:
                        data_tokens[fields_present.index(field_ref)].data[record_token.field_sub_ref-1] = record_token.data
                    else:
                        fields_present.append(field_ref)
                        data_tokens.append(Token(record_token.type, record_token.path, record_token.field_ref,
                                                 record_token.field_sub_ref, record_token.field_ref_bin,
                                                 [None] * field.repetitions))
                        data_tokens[fields_present.index(field_ref)].data[record_token.field_sub_ref-1] = record_token.data

            batch.append((fields_present, data_tokens))
            batch_used_columns.update(fields_present)

            self.exported_records += 1


            # flush butches
            if self.exported_records % 100 == 0 or self.exported_records == self.records_count:
                write_batch()


            # progress
            if show_progress and (time.time() - eta_last_updated >= 1 or self.exported_records == self.records_count):
                padding = len(str(self.records_count))

                eta_last_updated = time.time()

                seconds_elapsed = eta_last_updated - start_time
                seconds_remaining = (self.records_count - self.exported_records) * (seconds_elapsed/self.exported_records)
                eta_string = " ETA: %d:%02d" % (seconds_remaining//60, seconds_remaining%60)

                formating_string = "%%%dd/%%d" % padding
                progress_info = formating_string % (self.exported_records, self.records_count)

                progress_info += eta_string

                if self.exported_records < self.records_count:
                    sys.stdout.write(progress_info)
                    sys.stdout.flush()
                    sys.stdout.write('\b' * len(progress_info))
                else:
                    sys.stdout.flush()

        output.close()
