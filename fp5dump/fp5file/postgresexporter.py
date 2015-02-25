from collections import OrderedDict
from io import BytesIO
import locale
from struct import pack
import sys
import time
import uuid
import psycopg2
import psycopg2.extras
from binascii import unhexlify

from .blockchain import decode_vli, decode_field_and_sub_ref, encode_vli
from .exporter import Exporter


class PostgresExporter(Exporter):
    def __init__(self, fp5file, export_definition, schema, psycopg2_connect_string,
                 first_record_to_process=None, update_table=False, table_name=None, show_progress=False, drop_empty_columns=False):
        super(PostgresExporter, self).__init__(fp5file, export_definition, first_record_to_process, table_name, show_progress, drop_empty_columns)

        self.schema = schema
        self.update_table = update_table
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

                    self.logging.debug("created enum '%s'" % enum_name)
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

        if not self.update_table:
            if not self.create_table(conn):
                return False
        else:
            if self.update_table:
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

    def delete_records(self, conn):
        self.logging.info("checking for records to delete")

        with conn.cursor() as cursor:
            temp_table_name = "%s_del" % self.table_name

            # cursor.execute('CREATE TEMP TABLE "%s" ("fm_id" bigint);' % temp_table_name)
            cursor.execute('CREATE TABLE "%s" ("fm_id" bigint PRIMARY KEY);' % temp_table_name)

            cope_stream = BytesIO()
            cope_stream.write(pack('>11sii', b'PGCOPY\n\377\r\n\0', 0, 0))

            for fm_id in self.fp5file.records_index:
                cope_stream.write(pack('>hiq', 1, 8, fm_id))
                # cursor.execute('INSERT INTO "%s" VALUES (%s);' % (temp_table_name, fm_id))

            cope_stream.write(pack('!h', -1))
            cope_stream.seek(0)

            cursor.copy_expert('COPY "%s" FROM STDIN WITH BINARY' % (temp_table_name), cope_stream)

            cursor.execute('DELETE FROM "%s"."%s" WHERE fm_id NOT IN (SELECT * FROM "%s");' % (self.schema, self.table_name, temp_table_name))

            self.deleted_records = cursor.rowcount

            conn.commit()

        self.logging.info("records deleted")

    def pre_run_actions(self, conn):
        self.set_locale()

        psycopg2.extras.register_uuid()

        if self.table_name is None:
            self.table_name = self.fp5file.db_name

        self.create_schema(conn)
        self.create_table_and_enums(conn)

        self.start_time = self.eta_last_updated = time.time()

        self.records_to_process_count = self.fp5file.records_count

        if self.first_record_to_process is not None:
            self.records_to_process_count -= self.fp5file.records_index.index(self.first_record_to_process)

        if self.update_table:
            self.delete_records(conn)

    def flush_batch(self, conn, batch_values, batch_fields_present):
        with conn.cursor() as cursor:
            insert_prepare_statement = 'PREPARE batch_insert AS INSERT INTO "%s"."%s" ("fm_id", "fm_mod_id", ' % (self.schema, self.table_name)
            insert_prepare_statement += ', '.join('"%s"' % self.export_definition[field_ref].field.label for field_ref in batch_fields_present)
            insert_prepare_statement += ') VALUES ($1, $2'

            update_prepare_statement = 'PREPARE batch_update AS UPDATE "%s"."%s" SET "fm_mod_id" = $2' % (self.schema, self.table_name)

            insert_execute_statement = 'EXECUTE batch_insert (%s, %s'
            update_execute_statement = 'EXECUTE batch_update (%s, %s'

            for i, field_ref in enumerate(batch_fields_present):
                export_def = self.export_definition[field_ref]

                if export_def.is_enum or export_def.is_array:
                    insert_execute_statement += ', %%s%s' % export_def.psql_cast
                    update_execute_statement += ', %%s%s' % export_def.psql_cast
                else:
                    insert_execute_statement += ', %s'
                    update_execute_statement += ', %s'

                insert_prepare_statement += ', $%d' % (i + 3)
                update_prepare_statement += ', "%s" = $%d' % (self.export_definition[field_ref].field.label, i + 3)

            insert_prepare_statement += ');'
            update_prepare_statement += ' WHERE "fm_id" = $1;'

            insert_execute_statement += ');'
            update_execute_statement += ');'

            cursor.execute(insert_prepare_statement)
            cursor.execute(update_prepare_statement)

            batch_fields_count = len(batch_fields_present) + 2
            batch_fields_count_check = batch_fields_count + 1

            for record_values in batch_values:
                update = record_values[0]
                record_values = record_values[1:] + [None] * (batch_fields_count_check - len(record_values))

                if not update:
                    cursor.execute(insert_execute_statement, record_values)
                else:
                    cursor.execute(update_execute_statement, record_values)

            cursor.execute('DEALLOCATE batch_insert;')
            cursor.execute('DEALLOCATE batch_update;')

            conn.commit()

    def run(self):
        try:
            with psycopg2.connect(self.psycopg2_connect_string) as conn:
                with conn.cursor() as cursor:
                    self.pre_run_actions(conn)

                    batch_values = []
                    batch_fields_present = []
                    table_fields_present = set()

                    cursor.execute("""PREPARE get_mod_id AS SELECT "fm_mod_id" FROM "%s"."%s" WHERE "fm_id" = $1;""" % (self.schema, self.table_name))

                    if self.first_record_to_process is not None:
                        start_node_path = b'\x05/' + encode_vli(self.first_record_to_process)
                    else:
                        start_node_path = None

                    for (record_id_bin, record_tokens) in self.fp5file.data.subnodes(b'\x05', start_node_path=start_node_path):
                        # progress counter
                        self.update_progress()

                        # get basic record infos
                        record_id = decode_vli(record_id_bin)
                        mod_id = int.from_bytes(record_tokens[b'\xfc'], byteorder='big') if b'\xfc' in record_tokens else 0
                        update_record = False

                        # check if insert/update/skip
                        if self.update_table:
                            cursor.execute('execute get_mod_id(%s);', (record_id,))

                            mod_id_check = cursor.fetchone()

                            if mod_id_check is not None:
                                if mod_id == mod_id_check[0]:
                                    continue

                                update_record = True

                        # prepare values for insert/update statement
                        values = [update_record, record_id, mod_id] + ([None] * len(batch_fields_present))

                        for (field_ref_bin, field_value) in record_tokens.items():
                            (field_ref, sub_field_ref) = decode_field_and_sub_ref(field_ref_bin)

                            export_def = None

                            if field_ref:
                                for _export_def in self.export_definition.values():
                                    if field_ref == _export_def.field.id:
                                        export_def = _export_def

                                        if field_ref not in batch_fields_present:
                                            batch_fields_present.append(field_ref)
                                            values.append(None)

                                        break

                            if export_def:
                                if type(field_value) is OrderedDict:
                                    if b'\x00\x00' in field_value:
                                        field_value = field_value[b'\x00\x00']

                                value = field_value.decode(self.fp5file.encoding)

                                value_pos = batch_fields_present.index(export_def.field_id) + 3

                                if export_def.split:
                                    values[value_pos] = value.splitlines()
                                elif export_def.subscript is not None:
                                    if sub_field_ref == export_def.subscript:
                                        values[value_pos] = value
                                elif not export_def.is_array:
                                    values[value_pos] = value
                                else:
                                    if values[value_pos] is None:
                                        values[value_pos] = [None] * export_def.field.repetitions

                                    values[value_pos][sub_field_ref - 1] = value

                        # convert values
                        for field_ref in batch_fields_present:
                            value_pos = batch_fields_present.index(field_ref) + 3
                            export_def = self.export_definition[field_ref]

                            try:
                                values[value_pos] = self.values_for_field_type(values[value_pos], export_def)
                            except ValueError:
                                self.aggregate_errors(values, export_def, values[value_pos], batch_fields_present)

                                values[value_pos] = None

                        # push values
                        batch_values.append(values)

                        if update_record:
                            self.updated_records += 1
                        else:
                            self.inserted_records += 1

                        # flush
                        if len(batch_values) >= 100:
                            self.flush_batch(conn, batch_values, batch_fields_present)

                            table_fields_present.update(batch_fields_present)
                            batch_fields_present.clear()
                            batch_values.clear()

                    # final flush
                    if batch_values:
                        self.flush_batch(conn, batch_values, batch_fields_present)

                        table_fields_present.update(batch_fields_present)
                        batch_fields_present.clear()
                        batch_values.clear()

                    # drop empty column
                    if self.drop_empty_columns and not self.update_table:
                        with conn.cursor() as cursor:
                            for export_def in self.export_definition.values():
                                if export_def.field_id not in table_fields_present:
                                    cursor.execute('ALTER TABLE "%s" DROP COLUMN  "%s";\n' % (self.table_name, export_def.field.label))

                    # deallocate prepares statement
                    cursor.execute("""DEALLOCATE get_mod_id;""")

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

        if not self.update_table:
            self.logging.info("inserted %d records" % self.exported_records)
        else:
            self.logging.info("inserted %d / updated %d / deleted %d / processed %d records" % (self.inserted_records, self.updated_records, self.deleted_records, self.processed_records))

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