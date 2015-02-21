from collections import namedtuple, OrderedDict
import locale
import os
import re
import struct
import logging
import yaml
import codecs
import yaml.constructor

from array import array
import parsedatetime as pdt
from binascii import hexlify, unhexlify

from .block import TokenType, decode_vli
from .blockchain import BlockChain, BlockChainIter
from .datafield import DataField

from .psqlexporter import PsqlExporter
from .postgresexporter import PostgresExporter


class FP5File(object):
    """Wrapper for FP5 file object"""

    def __init__(self, filename, encoding=None, locale=None):
        super(FP5File, self).__init__()

        self.logging = logging.getLogger('fp5dump.fp5file.fp5file')

        self.filename = filename

        self.locale = locale
        self.encoding = encoding if encoding else 'latin1'
        self.ptd_parser = pdt.Calendar(pdt.Constants())

        self.basename = os.path.splitext(os.path.basename(filename))[0]
        self.dirname = os.path.dirname(os.path.abspath(os.path.expanduser(filename)))
        self.db_name = self.basename

        self.export_definition = None

        self.error_report_columns = []

        self.block_chains = []
        self.block_chain_levels = 0
        self.fields = {}

        self.index = None
        self.data = None

        self.enums = []

        self.file_size = 0

        self.records_index = []
        self.records_count = 0

        self.version_string = ""
        self.filename_string = ""
        self.server_addr_string = ""

        self.block_prev_id_to_block_pos = None
        self.block_id_to_block_pos = None

        self.logging.info("opening %s" % self.basename)

        self.file = open(os.path.abspath(os.path.expanduser(self.filename)), "rb", buffering=0)

        self.largest_block_id = 0x00000000

        self.read_header()
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

    def read_header(self):
        if not self.read_header_fp5():
            if not self.read_header_fp3():
                self.logging.error("could not read a valid fp5 or fp3 header")

    def read_header_fp3(self):
        self.file_size = os.path.getsize(self.filename)

        if self.file_size % 0x400 != 0:
            raise Exception("File size is not a multiple of 0x400")

        self.file.seek(0)
        magic = self.file.read(0x0F)

        unknown1 = self.file.read(0x1F1)
        unknown2 = self.file.read(0x0D)
        hbam = self.file.read(0x0D)
        unknown3 = self.file.read(0x03)

        version_string_length = int.from_bytes(self.file.read(0x01), byteorder='big')
        self.version_string = self.file.read(version_string_length)

        unknown4 = self.file.read(0x02)
        unknown5 = self.file.read(0x1BA - version_string_length)
        copyright_string = self.file.read(0x26)

        filename_string_length = int.from_bytes(self.file.read(0x01), byteorder='big')
        self.filename_string = self.file.read(filename_string_length)
        unknown6 = self.file.read(0xFF - filename_string_length)

        server_addr_string_length = int.from_bytes(self.file.read(0x01), byteorder='big')
        self.server_addr_string = self.file.read(server_addr_string_length)
        unknown7 = self.file.read(0x2FF - server_addr_string_length)

        if magic != unhexlify(b'0001000000020001000500020002C0'):
            self.logging.error("unexpected magic number %s" % magic)
            return False

        if self.version_string != b'Pro 3.0':
            self.logging.error("unexpected version string %s\n"
                               "if this string seems legitimate please report this as an issue" % self.version_string)
            return False

        return True

    def read_header_fp5(self):
        self.file_size = os.path.getsize(self.filename)

        if self.file_size % 0x400 != 0:
            raise Exception("File size is not a multiple of 0x400")

        self.file.seek(0)
        magic = self.file.read(0x0F)

        unknown1 = self.file.read(0x1CB)
        copyright_string = self.file.read(0x25)
        unknown2 = self.file.read(0x0E)
        hbam = self.file.read(0x0D)
        unknown3 = self.file.read(0x03)

        version_string_length = int.from_bytes(self.file.read(0x01), byteorder='big')
        self.version_string = self.file.read(version_string_length)
        version_string_padding = self.file.read(0x1E2 - version_string_length)

        filename_string_length = int.from_bytes(self.file.read(0x01), byteorder='big')
        self.filename_string = self.file.read(filename_string_length)
        filename_string_padding = self.file.read(0xFF - filename_string_length)

        server_addr_string_length = int.from_bytes(self.file.read(0x01), byteorder='big')
        self.server_addr_string = self.file.read(server_addr_string_length)
        server_addr_string_length = self.file.read(0xBF - server_addr_string_length)

        unknown4 = self.file.read(0x0C)
        unknown5 = self.file.read(0x234)

        if magic != unhexlify(b'0001000000020001000500020002C0'):
            self.logging.error("unexpected magic number %s" % magic)

            return False

        if self.version_string != b'Pro 5.0':
            if self.version_string != b'Pro 3.0':
                self.logging.error("unexpected version string %s\n"
                                   "if this string seems legitimate please report this as an issue" % self.version_string)
            return False

        return True

    def generate_export_definition(self,
                                   include_fields=[],
                                   include_fields_like=[],
                                   ignore_fields=[],
                                   ignore_fields_like=[],
                                   ignore_field_types=['GLOBAL', 'CONTAINER'],
                                   treat_all_as_string=False,
                                   use_locale="en_US",
                                   encoding="latin1"):

        try:
            locale.setlocale(locale.LC_NUMERIC, use_locale)
            locale.resetlocale(locale.LC_NUMERIC)

            self.locale = use_locale

        except locale.Error:
            self.logging.error("invalid locale '%s' specified" % use_locale)

            return None

        try:
            codecs.lookup(encoding)

            if encoding != self.encoding:
                self.encoding = encoding

                for field in self.fields.values():
                    field.label = field.label_bytes.decode(self.encoding)

        except LookupError:
            self.logging.error("invalid encoding '%s' specified" % encoding)

            return None

        export_definition = OrderedDict()

        field_pos = 2
        for field_id in sorted(self.fields.keys()):
            field = self.fields[field_id]

            include = False
            exclude = False

            for include_fields_name in include_fields:
                if include_fields_name == field.label:
                    include = True

            for include_field_reg in include_fields_like:
                if re.search(include_field_reg, field.label):
                    include = True

            if not include_fields and not include_fields_like:
                include = True

            for ignore_field_name in ignore_fields:
                if ignore_field_name == field.label:
                    exclude = True

            for ignore_field_reg in ignore_fields_like:
                if re.search(ignore_field_reg, field.label):
                    exclude = True

            if field.typename in ignore_field_types:
                exclude = True

            if not field.stored:
                exclude = True

            if include and not exclude:
                export_definition[field_id] = FieldExportDefinition(field_id, field,
                                                                    field.psql_type if not treat_all_as_string else "text",
                                                                    field.psql_type if not treat_all_as_string else "text",
                                                                    field.psql_cast if not treat_all_as_string else ("::text" "[]" if self.repetitions == 0 else ":text[]"),
                                                                    field.repetitions > 1, False, None, False, None, field_pos)

                field_pos += 1

        return export_definition

    def load_export_definition(self, yaml_file_path):
        export_definition = OrderedDict()

        with open(os.path.abspath(os.path.expanduser(yaml_file_path)), 'r') as f:
            yaml_definition = yaml.load(f, __OrderedDictYAMLLoader__)

        if yaml_definition:
            if 'name' in yaml_definition:
                self.db_name = yaml_definition['name']

            if 'locale' in yaml_definition:
                try:
                    locale.setlocale(locale.LC_NUMERIC, yaml_definition['locale'])
                    locale.resetlocale(locale.LC_NUMERIC)

                    self.locale = yaml_definition['locale']

                except locale.Error:
                    self.logging.error("invalid locale '%s' specified" % yaml_definition['locale'])

                    return None


            if 'encoding' in yaml_definition:
                try:
                    codecs.lookup(yaml_definition['encoding'])

                    if yaml_definition['encoding'] != self.encoding:
                        self.encoding = yaml_definition['encoding']

                        for field in self.fields.values():
                            field.label = field.label_bytes.decode(self.encoding)

                except LookupError:
                    self.logging.error("invalid encoding '%s' specified in export definition yaml" % (
                        yaml_definition['encoding']))

                    return None

            field_pos = 2
            for (column_name, column_type) in yaml_definition['columns'].items():
                column_type = column_type.strip()

                if column_type.startswith("bool") and not column_type.startswith("boolean"):
                    column_type = "boolean%s" % column_type[4:]
                elif column_type.startswith("int") and not column_type.startswith("integer"):
                    column_type = "integer%s" % column_type[3:]

                    yaml_definition['columns'][column_name] = column_type

                for field_id in sorted(self.fields.keys()):
                    field = self.fields[field_id]

                    if field.stored and field.label == column_name:
                        field_def = {
                            "field_id": field_id,
                            "type": column_type,
                            "psql_type": None,
                            "psql_cast": "",
                            "is_array": False,
                            "split": False,
                            "subscript": None,
                            "is_enum": False,
                            "enum": None,
                            "pos": field_pos
                        }

                        subscript_check = re.compile('^(.+)\[(\d+)\]$')
                        enum_check = re.compile('^enum\("(.+)"\)$')

                        if column_type.endswith("[]"):
                            if field.repetitions > 1:
                                field_def['psql_type'] = column_type[:-2]
                                field_def['is_array'] = True
                            else:
                                field_def['psql_type'] = column_type[:-2]
                                field_def['split'] = True
                                field_def['is_array'] = True

                        elif subscript_check.match(column_type):
                            if field.repetitions > 1:
                                (field_def['psql_type'], field_def['subscript']) = \
                                    subscript_check.match(column_type).groups()
                            else:
                                self.logging.warning("subscript specified (%s) for non array field %s" % (
                                    field_def['type'], field.label))

                                field_def['psql_type'] = subscript_check.match(column_type).group(1)
                        else:
                            if field.repetitions > 1:
                                self.logging.warning(
                                    "%s is an array field of length %d. only first value will be exported" % (
                                        field.label, field.repetitions))

                                field_def['psql_type'] = column_type
                                field_def['subscript'] = 0
                            else:
                                field_def['psql_type'] = column_type

                        if enum_check.match(field_def['psql_type']):
                            field_def['psql_type'] = enum_check.match(field_def['psql_type']).group(1)
                            field_def['is_enum'] = True

                            if 'enums' not in yaml_definition or \
                                    ('enums' in yaml_definition and
                                        field_def['psql_type'] not in yaml_definition['enums']):
                                self.logging.error(
                                    "undefined enum '%s' found in export definition for field '%s'" % (
                                        field_def['psql_type'], field.label))

                                return None

                            field_def['enum'] = {}

                            for enum_key, enum_values in yaml_definition['enums'][field_def['psql_type']].items():
                                field_def['enum'][enum_key] = \
                                    [(v.upper() if v is not None else None) for v in (enum_values if type(enum_values) is list else [enum_values])]

                            if '*' in field_def['enum']:
                                field_def['enum']['*'] = field_def['enum']['*'][0]

                        elif field_def['psql_type'] not in ['integer', 'numeric', 'text', 'boolean', 'date', 'uuid']:
                            self.logging.error("unexpected type '%s' found in export definition for field '%s'" % (
                                field_def['psql_type'], field.label))

                            return None

                        if field_def['is_array'] and field_def['is_enum']:
                            field_def['psql_cast'] = "::\"%s\"[]" % (field_def['psql_type'])
                        elif field_def['is_enum']:
                            field_def['psql_cast'] = "::\"%s\"" % (field_def['psql_type'])
                        elif field_def['is_array']:
                            field_def['psql_cast'] = "::%s[]" % (field_def['psql_type'])

                        export_definition[field_def['field_id']] = FieldExportDefinition(
                                field_def['field_id'], field,
                                field_def['type'], field_def['psql_type'], field_def['psql_cast'],
                                field_def['is_array'], field_def['split'], field_def['subscript'],
                                field_def['is_enum'], field_def['enum'], field_def['pos'])

                        field_pos += 1

                self.error_report_columns = []

                if 'error_report_columns' in yaml_definition:
                    for error_report_column_label in yaml_definition['error_report_columns']:
                        for field_id in sorted(self.fields.keys()):
                            if error_report_column_label == self.fields[field_id].label and field_id in export_definition:
                                self.error_report_columns.append(export_definition[field_id])

        return export_definition

    def get_blocks(self):
        try:
            pos = 0x800

            while pos < self.file_size:
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
                            self.logging.error("block with duplicate prev_id 0x%08X found for level %d" % (prev_id, level))

                pos += 0x400

        except Exception as e:
            raise e

        self.logging.info("blocks read")

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
            output_filename = self.filename + ".data"

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
            if token.type != TokenType.xC0:
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

            if token.type == TokenType.xC0 and len(token_path_split) == search_path_split_len + 1:
                if last_sub_data_key is not None and last_sub_data_start < len(tokens):
                    yield tokens[last_sub_data_start:] + [token]

                    del tokens[last_sub_data_start:]
                else:
                    yield [token]

            if sub_data_key != last_sub_data_key:
                if sub_data_key is not None:
                    last_sub_data_start = len(tokens)

                last_sub_data_key = sub_data_key

            if token.type != TokenType.xC0:
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
                            field.label_bytes = option_token.data

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

    def insert_records_into_postgres(self, fields_to_dump, first_record_to_export=None, table_name=None,
                                     psycopg2_connect_string=None, schema=None, show_progress=False,
                                     drop_empty_columns=False):
        self.logging.info("inserting records")

        exporter = PostgresExporter(self, fields_to_dump,
                                    schema, psycopg2_connect_string,
                                    first_record_to_export=first_record_to_export,
                                    use_existing_table=False,
                                    table_name=table_name,
                                    drop_empty_columns=drop_empty_columns,
                                    show_progress=show_progress)
        exporter.run()

        if exporter.sampled_errors_for_fields:
            print(exporter.format_errors())

    def update_records_into_postgres(self, fields_to_dump, first_record_to_export=None, table_name=None,
                                     psycopg2_connect_string=None, schema=None, show_progress=False,
                                     drop_empty_columns=False):
        self.logging.info("updating records")

        exporter = PostgresExporter(self, fields_to_dump,
                                    schema, psycopg2_connect_string,
                                    first_record_to_export=first_record_to_export,
                                    use_existing_table=True,
                                    table_name=table_name,
                                    drop_empty_columns=drop_empty_columns,
                                    show_progress=show_progress)
        exporter.run()

        if exporter.sampled_errors_for_fields:
            print(exporter.format_errors())

    def dump_records_pgsql(self, fields_to_dump, first_record_to_export=None, filename=None, table_name=None,
                           show_progress=False, drop_empty_columns=False):
        self.logging.info("dumping records")

        if filename is None:
            filename = os.path.join(self.dirname, self.basename + '.psql')

        exporter = PsqlExporter(self, fields_to_dump, filename,
                                first_record_to_export=first_record_to_export,
                                table_name=table_name,
                                drop_empty_columns=drop_empty_columns,
                                show_progress=show_progress)
        exporter.run()

        if exporter.sampled_errors_for_fields:
            print(exporter.format_errors())


FieldExportDefinition = namedtuple('FieldExportDefinition', ["field_id", "field", "type", "psql_type", "psql_cast",
                                                             "is_array", "split", "subscript",
                                                             "is_enum", "enum", "pos"])


class __OrderedDictYAMLLoader__(yaml.Loader):
    """
    A YAML loader that loads mappings into ordered dictionaries.
    """

    def __init__(self, *args, **kwargs):
        yaml.Loader.__init__(self, *args, **kwargs)

        self.add_constructor(u'tag:yaml.org,2002:map', type(self).construct_yaml_map)
        self.add_constructor(u'tag:yaml.org,2002:omap', type(self).construct_yaml_map)

    def construct_yaml_map(self, node):
        data = OrderedDict()
        yield data
        value = self.construct_mapping(node)
        data.update(value)

    def construct_mapping(self, node, deep=False):
        if isinstance(node, yaml.MappingNode):
            self.flatten_mapping(node)
        else:
            raise yaml.constructor.ConstructorError(None, None,
                'expected a mapping node, but found %s' % node.id, node.start_mark)

        mapping = OrderedDict()
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                hash(key)
            except TypeError as exc:
                raise yaml.constructor.ConstructorError('while constructing a mapping',
                    node.start_mark, 'found unacceptable key (%s)' % exc, key_node.start_mark)
            value = self.construct_object(value_node, deep=deep)
            mapping[key] = value
        return mapping
