import locale
import os
import sys
import time
from binascii import unhexlify
import uuid

from .blockchain import decode_vli
from .exporter import Exporter


class PsqlExporter(Exporter):
    def __init__(self, fp5file, export_definition, filename,
                 first_record_to_process=None, table_name=None, show_progress=False, drop_empty_columns=False):
        super(PsqlExporter, self).__init__(fp5file, export_definition, first_record_to_process, table_name, show_progress, drop_empty_columns)

        self.filename = filename

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

    def create_enum(self, output, enum_name, enum_def):
        output.write('CREATE TYPE "%s" AS ENUM(%s); \n' % (enum_name, (", ".join("'%s'" % key for (key, value) in enum_def.items() if key != "*"))))

    def create_table(self, output):
        pgsql_fields = [' "fm_id" bigint', ' "fm_mod_id" bigint']

        for export_def in self.export_definition.values():
            if export_def.is_array and export_def.is_enum:
                pgsql_fields.append(' "%s" "%s"[]' % (export_def.field.label, export_def.psql_type))
            elif export_def.is_enum:
                pgsql_fields.append(' "%s" "%s"' % (export_def.field.label, export_def.psql_type))
            elif export_def.is_array:
                pgsql_fields.append(' "%s" %s[]' % (export_def.field.label, export_def.psql_type))
            else:
                pgsql_fields.append(' "%s" %s' % (export_def.field.label, export_def.psql_type))

        pgsql_fields.append('  CONSTRAINT "_%s_pkey" PRIMARY KEY ("fm_id")' % self.table_name)

        output.write('DROP TABLE IF EXISTS "%s";\n' % self.table_name)
        output.write('CREATE TABLE IF NOT EXISTS "%s" (\n%s\n);\n\n' % (self.table_name, ',\n'.join(pgsql_fields)))

    def create_table_and_enums(self, output):
        handeled_enums = set()

        for export_def in self.export_definition.values():
            if export_def.is_enum and export_def.psql_type not in handeled_enums:
                self.create_enum(output, export_def.psql_type, export_def.enum)

                handeled_enums.add(export_def.psql_type)

        self.create_table(output)

    def pre_run_actions(self, output):
        self.set_locale()

        if self.table_name is None:
            self.table_name = self.fp5file.db_name

        self.create_table_and_enums(output)

        self.start_time = self.eta_last_updated = time.time()

        self.records_to_process_count = self.fp5file.records_count

        if self.first_record_to_process is not None:
            self.records_to_process_count -= self.fp5file.records_index.index(self.first_record_to_process)

    def flush_batch(self, output, batch_values, batch_fields_present):
        output.write('INSERT INTO "%s" ("fm_id", "fm_mod_id", ' % self.table_name)
        output.write(', '.join('"%s"' % self.export_definition[field_ref].field.label for field_ref in batch_fields_present))
        output.write(') VALUES (\n\t')

        batch_count = len(batch_values)
        batch_counter = 0

        for record_values in batch_values:
            output.write("%d,\n\t" % record_values[0])
            output.write("%d,\n\t" % record_values[1])

            i = 2
            last_record_value_index = len(record_values) - 1
            last_batch_index = len(batch_fields_present) + 2 - 1

            while i <= last_batch_index:
                if i <= last_record_value_index:
                    output.write(record_values[i])
                else:
                    output.write("NULL")

                if i != last_batch_index:
                    output.write(",\n\t")

                i += 1

            batch_counter += 1

            output.write('\n), (\n\t' if batch_counter != batch_count else '\n);\n\n')

    def run(self):
        with open(os.path.abspath(os.path.expanduser(self.filename)), "w", encoding="utf8") as output:
            self.pre_run_actions(output)

            batch_values = []
            batch_fields_present = []
            table_fields_present = set()

            for record_tokens in self.fp5file.get_sub_data_with_path(b'05', first_sub_record_to_export=self.first_record_to_process):
                record_id = unhexlify(record_tokens[0].path.split(b'/')[1])
                record_id = decode_vli(record_id)

                record_path = b'/'.join(record_tokens[0].path.split(b'/')[:2])

                values = [record_id, 0] + ([None] * len(batch_fields_present))
                #
                # for record_token in record_tokens:
                #     export_def = None
                #     field_ref = None
                #
                #     if record_token.type == TokenType.xC0:
                #         continue
                #     elif record_token.type == TokenType.xFC:
                #         values[1] = int.from_bytes(record_token.data, byteorder='big')
                #
                #         continue
                #     elif record_path == record_token.path:
                #         field_ref = record_token.field_ref
                #     elif len(record_token.path.split(b'/')) == 3:
                #         field_ref = decode_vli(unhexlify(b'/'.join(record_token.path.split(b'/')[2:])))
                #     else:
                #         continue
                #
                #     if field_ref:
                #         for _export_def in self.export_definition.values():
                #             if field_ref == _export_def.field.id:
                #                 export_def = _export_def
                #
                #                 if field_ref not in batch_fields_present:
                #                     batch_fields_present.append(field_ref)
                #                     values.append(None)
                #
                #                 break
                #
                #     if export_def:
                #         value = record_token.data.decode(self.fp5file.encoding)
                #
                #         value_pos = batch_fields_present.index(export_def.field_id) + 2
                #
                #         if export_def.split:
                #             values[value_pos] = value.splitlines()
                #         elif export_def.subscript is not None:
                #             if record_token.field_sub_ref == export_def.subscript:
                #                 values[value_pos] = value
                #         elif not export_def.is_array:
                #             values[value_pos] = value
                #         else:
                #             if values[value_pos] is None:
                #                 values[value_pos] = [None] * export_def.field.repetitions
                #
                #             values[value_pos][record_token.field_sub_ref - 1] = value

                for field_ref in batch_fields_present:
                    value_pos = batch_fields_present.index(field_ref) + 2
                    export_def = self.export_definition[field_ref]

                    try:
                        values[value_pos] = self.values_for_field_type(values[value_pos], export_def)
                    except ValueError:
                        self.aggregate_errors(values, export_def, values[value_pos], batch_fields_present)

                        values[value_pos] = "NULL"

                batch_values.append(values)

                self.exported_records += 1

                if self.exported_records % 100 == 0 or self.exported_records == self.records_to_process_count:
                    self.flush_batch(output, batch_values, batch_fields_present)

                    table_fields_present.update(batch_fields_present)
                    batch_fields_present.clear()
                    batch_values.clear()

                if self.show_progress and (self.exported_records % 100 == 0):
                    self.update_progress()

            if self.drop_empty_columns:
                for export_def in self.export_definition.values():
                    if export_def.field_id not in table_fields_present:
                        output.write('ALTER TABLE "%s" DROP COLUMN  "%s";\n' % (self.table_name, export_def.field.label))

        self.reset_locale()

        sys.stdout.flush()
        self.logging.info("exported %d records" % self.exported_records)


    def values_for_field_type(self, value, export_def):
        if type(value) is list:
            sub_values = []

            for sub_value in value:
                if sub_value is not None and sub_value != '':
                    sub_values.append(self.values_for_field_type(sub_value, export_def))
                else:
                    sub_values.append("NULL")

            return "ARRAY[" + ", ".join(sub_values) + "]" + export_def.psql_cast

        if value is None:
            return "NULL"

        if export_def.psql_type == "text":
            return "E'%s'" % value.translate(self.trans)
        elif export_def.psql_type == "integer":
            return str(int(value))
        elif export_def.psql_type == "numeric":
            return str(locale.atof(value))
        elif export_def.psql_type == "date":
            date, check = self.ptd_parser.parseDT(value)

            if not check:
                raise ValueError

            return "'" + str(date.date()) + "'"
        elif export_def.psql_type == "uuid":
            return "'" + str(uuid.UUID(value)) + "'"
        elif export_def.psql_type == "boolean":
            if value.lower() in ('ja', 'yes', 'true', '1', 'ok'):
                return "TRUE"
            elif value.lower() in ('nein', 'no', 'false', '0', ''):
                return "FALSE"
            else:
                raise ValueError
        elif export_def.is_enum:
            catch_all = None

            for enum_key, enum_value in export_def.enum.items():
                if '*' == enum_key:
                    catch_all = enum_key
                elif value.upper() in enum_value:
                    return ("E'%s'" % enum_key.translate(self.trans) if enum_key is not None else "NULL") + export_def.psql_cast

            if catch_all:
                return ("E'%s'" % export_def.enum[catch_all].translate(self.trans) if export_def.enum[catch_all] is not None else "NULL") + export_def.psql_cast

            raise ValueError

        return "NULL"