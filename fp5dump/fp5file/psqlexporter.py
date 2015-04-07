from collections import OrderedDict, namedtuple
import locale
import os
import sys
import time
from binascii import unhexlify
import uuid

from .blockchain import decode_vli, encode_vli, split_field_and_sub_ref
from .exporter import Exporter


class PsqlExporter(Exporter):
    def __init__(self, fp5file, export_definition, filename,
                 first_record_to_process=None, table_name=None, show_progress=False, drop_empty_columns=False):
        super(PsqlExporter, self).__init__(fp5file, export_definition, first_record_to_process, table_name, show_progress, drop_empty_columns)

        self.filename = filename

        self.output = None
        self.insert_statement = ""
        self.trans = "".maketrans({
            '\\': '\\\\',
            '\'': '\\\'',
            '\b': '\\b',
            '\f': '\\f',
            '\n': '\\n',
            '\r': '\\r',
            '\t': '\\t',
            '\x00': ''
        })

    def create_enum(self, enum_name, enum_def):
        self.output.write('CREATE TYPE "%s" AS ENUM(); \n' % (enum_name))

        for enum_value in enum_def.keys():
            self.output.write("""ALTER TYPE "%s" ADD VALUE IF NOT EXISTS '%s';\n""" % (enum_name, enum_value))

    def create_table(self):
        pgsql_field_names = ['"fm_id"']
        pgsql_fields = [' "fm_id" bigint']

        for export_def in self.export_definition.values():
            pgsql_field_names.append('"%s"' % export_def.field.label)

            if export_def.is_array and export_def.is_enum:
                pgsql_fields.append(' "%s" "%s"[]' % (export_def.field.label, export_def.psql_type))
            elif export_def.is_enum:
                pgsql_fields.append(' "%s" "%s"' % (export_def.field.label, export_def.psql_type))
            elif export_def.is_array:
                pgsql_fields.append(' "%s" %s[]' % (export_def.field.label, export_def.psql_type))
            else:
                pgsql_fields.append(' "%s" %s' % (export_def.field.label, export_def.psql_type))

        pgsql_field_names.append('"fm_mod_id"')
        pgsql_fields.append(' "fm_mod_id" bigint')

        pgsql_fields.append('  CONSTRAINT "_%s_pkey" PRIMARY KEY ("fm_id")' % self.table_name)

        self.output.write('DROP TABLE IF EXISTS "%s";\n' % self.table_name)
        self.output.write('CREATE TABLE IF NOT EXISTS "%s" (\n%s\n);\n\n' % (self.table_name, ',\n'.join(pgsql_fields)))

        self.insert_statement = 'INSERT INTO "%s" (%s) VALUES \n(' % (self.table_name, ', '.join(pgsql_field_names))

    def create_table_and_enums(self):
        handeled_enums = set()

        for export_def in self.export_definition.values():
            if export_def.is_enum and export_def.psql_type not in handeled_enums:
                self.create_enum(export_def.psql_type, export_def.enum)

                handeled_enums.add(export_def.psql_type)

        self.create_table()

    def pre_run_actions(self):
        self.set_locale()

        if self.table_name is None:
            self.table_name = self.fp5file.db_name

        self.create_table_and_enums()

        self.start_time = self.eta_last_updated = time.time()

        self.records_to_process_count = self.fp5file.records_count

        if self.first_record_to_process is not None:
            self.records_to_process_count -= self.fp5file.records_index.index(self.first_record_to_process)

    def run(self):
        with open(os.path.abspath(os.path.expanduser(self.filename)), "w", encoding="utf8") as output:
            self.output = output

            self.pre_run_actions()

            if self.first_record_to_process is not None:
                start_node_path = b'\x05/' + encode_vli(self.first_record_to_process)
            else:
                start_node_path = None

            table_fields_present = set()
            token_ids_to_return = set(self.export_definition.keys())

            self.output.write(self.insert_statement)

            for (record_id_bin, record_tokens) in self.fp5file.data.sub_nodes(b'\x05', start_node_path=start_node_path, token_ids_to_return=token_ids_to_return):
                # progress counter
                self.update_progress()

                # get basic record infos
                record_id = decode_vli(record_id_bin)
                mod_id = int.from_bytes(record_tokens[b'\xfc'], byteorder='big') if b'\xfc' in record_tokens else 0

                output.write("%d, " % record_id)

                had_errors = False

                values = OrderedDict()

                for (field_id_combined_bin, field_value) in record_tokens.items():
                    field_id_bin, sub_field_id_bin = split_field_and_sub_ref(field_id_combined_bin)

                    if field_id_bin in self.export_definition:
                        export_def = self.export_definition[field_id_bin]

                        if export_def.field.repetitions > 1:
                            if not sub_field_id_bin:
                                sub_field_id_bin = b'\x01'

                            sub_field_id = decode_vli(sub_field_id_bin) - 1

                            if export_def.subscript is not None:
                                if sub_field_id == export_def.subscript:
                                    values[field_id_bin] = field_value
                            else:
                                if field_id_bin not in values:
                                    values[field_id_bin] = [None] * export_def.field.repetitions

                                values[field_id_bin][sub_field_id] = field_value
                        elif export_def.split:
                            values[field_id_bin] = field_value.splitlines()
                        else:
                            values[field_id_bin] = field_value

                for field_id_bin, export_def in self.export_definition.items():
                    if field_id_bin in values:
                        value = values[field_id_bin]

                        if self.drop_empty_columns:
                            table_fields_present.add(field_id_bin)

                        if type(value) is list:
                            self.output.write("ARRAY[")

                            for sub_value_index, sub_value in enumerate(value):
                                if sub_value_index != 0:
                                    output.write(", ")

                                if sub_value is not None and sub_value != '':
                                    if not self.values_for_field_type(sub_value, export_def):
                                        had_errors = True
                                        output.write("NULL")
                                        self.aggregate_errors(field_id_bin, record_id, sub_value)
                                else:
                                    self.output.write("NULL")

                            self.output.write("]" + export_def.psql_cast + ", ")
                        else:
                            if not self.values_for_field_type(value, export_def):
                                had_errors = True
                                output.write("NULL, ")
                                self.aggregate_errors(field_id_bin, record_id, values[field_id_bin])
                            else:
                                output.write(", ")
                    else:
                        output.write("NULL, ")

                if had_errors:
                    output.write("-1")
                else:
                    output.write("%d" % mod_id)

                if self.processed_records == self.records_to_process_count:
                    output.write(');\n\n')
                else:
                    output.write('),\n(' if self.processed_records % 1000 != 0 else ');\n\n' + self.insert_statement)

            if self.drop_empty_columns:
                for export_def in self.export_definition.values():
                    if export_def.field_id not in table_fields_present:
                        output.write('ALTER TABLE "%s" DROP COLUMN  "%s";\n' % (self.table_name, export_def.field.label))

        self.reset_locale()

        sys.stdout.flush()
        self.logging.info("exported %d records" % self.processed_records)

    def values_for_field_type(self, value, export_def):
        if type(value) is OrderedDict and b'\x01' in value and b'\xff\x00' in value:
            value = value[b'\x01'].decode(self.fp5file.encoding)
        else:
            value = value.decode(self.fp5file.encoding)

        if value is None:
            self.output.write("NULL")
            return True
        if export_def.psql_type == "text":
            self.output.write("E'%s'" % value.translate(self.trans))
            return True
        elif export_def.psql_type == "integer":
            try:
                self.output.write(str(int(value)))
                return True
            except ValueError:
                return False
        elif export_def.psql_type == "numeric":
            try:
                self.output.write(str(locale.atof(value)))
                return True
            except ValueError:
                return False
        elif export_def.psql_type == "date":
            date, check = self.ptd_parser.parseDT(value)

            if not check:
                return False

            self.output.write("'" + str(date.date()) + "'")
            return True
        elif export_def.psql_type == "uuid":
            self.output.write("'" + str(uuid.UUID(value)) + "'")
            return True
        elif export_def.psql_type == "boolean":
            if value.lower() in ('ja', 'yes', 'true', '1', 'ok'):
                self.output.write("TRUE")
                return True
            elif value.lower() in ('nein', 'no', 'false', '0', ''):
                self.output.write("FALSE")
                return True
            else:
                raise ValueError

        elif export_def.is_enum:
            value = value.upper()

            for enum_key, enum_value in export_def.enum.items():
                if '*' != enum_key and value in enum_value:
                    if enum_key == 'NULL':
                        self.output.write("NULL")
                    else:
                        self.output.write("E'%s'" % enum_key.translate(self.trans))

                    return True
            else:
                if '*' in export_def.enum:
                    catch_all = export_def.enum['*']

                    if catch_all is None or catch_all == 'NULL':
                        self.output.write("NULL")
                    else:
                        self.output.write("E'%s'" % catch_all.translate(self.trans))

                    return True
        return False
