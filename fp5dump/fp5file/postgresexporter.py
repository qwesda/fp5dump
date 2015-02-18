import locale
import sys
import time
import uuid
import psycopg2
import psycopg2.extras
from binascii import unhexlify

from .block import TokenType, decode_vli
from .exporter import Exporter


class PostgresExporter(Exporter):
    def __init__(self, fp5file, export_definition, schema, psycopg2_connect_string,
                 first_record_to_export=None, use_existing_table=False, table_name=None, show_progress=False, drop_empty_columns=False):
        super(PostgresExporter, self).__init__(fp5file, export_definition, first_record_to_export, table_name, show_progress, drop_empty_columns)

        self.schema = schema
        self.use_existing_table = use_existing_table
        self.psycopg2_connect_string = psycopg2_connect_string

    def create_enum(self, conn, enum_name, enum_def):
        try:
           with conn.cursor() as cursor:
                cursor.execute('SELECT enum_range(null::"%s")' % enum_name)
                existing_enum_values = cursor.fetchall()[0][0]

                for (key, value)in enum_def.items():
                    if key not in existing_enum_values and key != "*":
                        try:
                            conn.set_isolation_level(0)
                            cursor.execute('ALTER TYPE "%s" ADD VALUE \'%s\';' % (enum_name, key))
                            conn.commit()
                            conn.set_isolation_level(1)

                            self.logging.debug("added '%s' to enum '%s'" % (key, enum_name))
                        except Exception as e:
                            conn.rollback()

                            self.logging.error("could add value '%s' to enum '%s'\n\t%s" % (key, enum_name, e))

                            return False

        except Exception as e:
            conn.rollback()

            try:
               with conn.cursor() as cursor:
                    cursor.execute('CREATE TYPE "%s" AS ENUM(%s)' % (enum_name, (", ".join("'%s'" % key for (key, value) in enum_def.items()))))
                    conn.commit()

                    self.logging.debug("created enum '%s'" % (enum_name))
            except Exception as e:
                conn.rollback()

                self.logging.error("could not create enum '%s'\n\t%s" % (enum_name, e))

                return False

        return True

    def create_table(self, conn):
        try:
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

            pgsql_fields.append('CONSTRAINT "_%s_pkey" PRIMARY KEY ("fm_id")' % self.table_name)

            with conn.cursor() as cursor:
                cursor.execute('DROP TABLE IF EXISTS "%s";' % self.table_name)
                cursor.execute('CREATE TABLE IF NOT EXISTS "%s" (\n%s\n);\n\n' % (self.table_name, ',\n'.join(pgsql_fields)))

                conn.commit()

            self.logging.debug("created table '%s'" % self.table_name)
        except (psycopg2.OperationalError, psycopg2.ProgrammingError) as e:
            self.logging.error("could not create table '%s'\n\t%s" % (self.table_name, e))

            return False

        return True

    def create_table_and_enums(self, conn):
        handeled_enums = set()

        for export_def in self.export_definition.values():
            if export_def.is_enum and export_def.psql_type not in handeled_enums:
                if not self.create_enum(conn, export_def.psql_type, export_def.enum):
                    return False

        if not self.use_existing_table:
            if not self.create_table(conn):
                return False
        else:
            if self.use_existing_table:
                with conn.cursor() as cursor:
                    for field_def in self.export_definition.values():
                        field_to_export = self.fp5file.fields[field_def.field_id]

                        if field_def.is_enum:
                            cursor.execute('ALTER TABLE "%s" ALTER COLUMN "%s" TYPE %s USING "%s"::text%s;' % (
                                                self.table_name, field_to_export.label,
                                                field_def.psql_cast[2:], field_to_export.label,
                                                field_def.psql_cast)
                                            )
                            conn.commit()

        return True

    def create_schema(self, conn):
        with conn.cursor() as cursor:
            cursor.execute('CREATE SCHEMA IF NOT EXISTS "%s";' % self.schema)
            cursor.execute("SET SCHEMA '%s';" % self.schema)

            conn.commit()

    def pre_run_actions(self, conn):
        self.set_locale()

        psycopg2.extras.register_uuid()

        if self.table_name is None:
            self.table_name = self.fp5file.db_name

        self.create_schema(conn)
        self.create_table_and_enums(conn)

        self.start_time = self.eta_last_updated = time.time()

        self.records_to_export_count = self.fp5file.records_count

        if self.first_record_to_export is not None:
            self.records_to_export_count -= self.fp5file.records_index.index(self.first_record_to_export)

    def flush_batch(self, conn, batch_values, batch_fields_present):
        with conn.cursor() as cursor:
            insert_statement = 'INSERT INTO "%s" ("fm_id", "fm_mod_id", ' % self.table_name
            insert_statement += ', '.join('"%s"' % self.export_definition[field_ref].field.label for field_ref in batch_fields_present)
            insert_statement += ') VALUES '

            values_string = ''
            for field_ref in batch_fields_present:
                export_def = self.export_definition[field_ref]

                if export_def.is_enum or export_def.is_array:
                    values_string += '%%s%s, ' % export_def.psql_cast
                else:
                    values_string += '%s, '
            values_string = '(%%s, %%s, %s),\n' % values_string[:-2]

            batch_count = len(batch_values)
            batch_counter = 0

            batch_fields_count = len(batch_fields_present) + 2

            statement = insert_statement.encode()

            for record_values in batch_values:
                record_values = record_values + [None] * (batch_fields_count - len(record_values))

                batch_counter += 1

                if batch_counter == batch_count:
                    values_string = values_string[:-2]

                statement += cursor.mogrify(values_string, record_values)

            cursor.execute(statement)

    def run(self):
        try:
            with psycopg2.connect(self.psycopg2_connect_string) as conn:
                self.pre_run_actions(conn)

                batch_values = []
                batch_fields_present = []
                table_fields_present = set()

                for record_tokens in self.fp5file.get_sub_data_with_path(b'05', first_sub_record_to_export=self.first_record_to_export):
                    record_id = unhexlify(record_tokens[0].path.split(b'/')[1])
                    record_id = decode_vli(record_id)

                    record_path = b'/'.join(record_tokens[0].path.split(b'/')[:2])

                    values = [record_id, 0] + ([None] * len(batch_fields_present))

                    for record_token in record_tokens:
                        export_def = None
                        field_ref = None

                        if record_token.type == TokenType.xFC:
                            values[1] = int.from_bytes(record_token.data, byteorder='big')

                            continue
                        elif record_path == record_token.path:
                            field_ref = record_token.field_ref
                        elif len(record_token.path.split(b'/')) == 3:
                            field_ref = decode_vli(unhexlify(b'/'.join(record_token.path.split(b'/')[2:])))
                        else:
                            continue

                        if field_ref:
                            for _export_def in self.export_definition.values():
                                if field_ref == _export_def.field.id:
                                    export_def = _export_def

                                    if field_ref not in batch_fields_present:
                                        batch_fields_present.append(field_ref)
                                        values.append(None)

                                    break

                        if export_def:
                            value = record_token.data.decode(self.fp5file.encoding)

                            value_pos = batch_fields_present.index(export_def.field_id) + 2

                            if export_def.split:
                                values[value_pos] = value.splitlines()
                            elif export_def.subscript is not None:
                                if record_token.field_sub_ref == export_def.subscript:
                                    values[value_pos] = value
                            elif not export_def.is_array:
                                values[value_pos] = value
                            else:
                                if values[value_pos] is None:
                                    values[value_pos] = [None] * export_def.field.repetitions

                                values[value_pos][record_token.field_sub_ref - 1] = value

                    for field_ref in batch_fields_present:
                        value_pos = batch_fields_present.index(field_ref) + 2
                        export_def = self.export_definition[field_ref]

                        try:
                            values[value_pos] = self.values_for_field_type(values[value_pos], export_def)
                        except ValueError:
                            self.aggregate_errors(values, export_def, values[value_pos], batch_fields_present)

                            values[value_pos] = None

                    batch_values.append(values)

                    self.exported_records += 1

                    if self.exported_records % 100 == 0 or self.exported_records == self.records_to_export_count:
                        self.flush_batch(conn, batch_values, batch_fields_present)

                        table_fields_present.update(batch_fields_present)
                        batch_fields_present.clear()
                        batch_values.clear()

                    if self.show_progress and (self.exported_records % 100 == 0):
                        self.show_progress_info()

                if self.drop_empty_columns and not self.use_existing_table:
                    with conn.cursor() as cursor:
                        for export_def in self.export_definition.values():
                            if export_def not in table_fields_present:
                                cursor.execute('ALTER TABLE "%s" DROP COLUMN  "%s";\n' % (self.table_name, export_def.field.label))

        except (psycopg2.OperationalError, psycopg2.ProgrammingError) as psycopg_error:
            if psycopg_error.pgerror:
                sys.stdout.flush()
                self.logging.error(psycopg_error.pgerror)

            if psycopg_error.diag:
                sys.stdout.flush()
                self.logging.error(psycopg_error)

            self.reset_locale()

            return

        self.reset_locale()

        sys.stdout.flush()
        self.logging.info("exported %d records" % self.exported_records)


    def values_for_field_type(self, value, field_def):
        if type(value) is list:
            sub_values = []

            for sub_value in value:
                if sub_value is not None and sub_value != '':
                    sub_values.append(self.values_for_field_type(sub_value, field_def))
                else:
                    sub_values.append(None)

            return sub_values

        if value is None:
            return None

        if field_def.psql_type == "text":
            return value
        elif field_def.psql_type == "integer":
            return int(value)
        elif field_def.psql_type == "numeric":
            return locale.atof(value)
        elif field_def.psql_type == "date":
            date, check = self.ptd_parser.parseDT(value)

            if not check:
                raise ValueError

            return date
        elif field_def.psql_type == "uuid":
            return uuid.UUID(value)
        elif field_def.psql_type == "boolean":
            if value.lower() in ('ja', 'yes', 'true', '1', 'ok'):
                return True
            elif value.lower() in ('nein', 'no', 'false', '0', ''):
                return False
            else:
                raise ValueError
        elif field_def.is_enum:
            catch_all = None

            for enum_key, enum_value in field_def.enum.items():
                if '*' == enum_key:
                    catch_all = enum_key
                elif value.upper() in enum_value:
                    return enum_key if enum_key != 'NULL' else None

            if catch_all:
                return field_def.enum[catch_all] if field_def.enum[catch_all] != 'NULL' else None

            raise ValueError

        return None