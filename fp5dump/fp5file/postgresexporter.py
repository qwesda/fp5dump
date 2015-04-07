from collections import OrderedDict, namedtuple
from io import BytesIO
from struct import pack, pack_into
import sys
import time
import datetime
import uuid
import psycopg2
import psycopg2.extras
import struct
import re

from .blockchain import decode_vli, encode_vli, split_field_and_sub_ref
from .exporter import Exporter


FieldExportDefinition = namedtuple('FieldExportDefinition', ["field_id", "field", "type", "psql_type", "psql_cast", "pg_oid",
                                                             "is_array", "split", "subscript",
                                                             "is_enum", "enum", "pos"])


class PostgresExporter(Exporter):
    def __init__(self, fp5file, export_definition, schema, psycopg2_connect_string,
                 first_record_to_process=None, update_table=False, table_name=None, show_progress=False, drop_empty_columns=False):
        super(PostgresExporter, self).__init__(fp5file, export_definition, first_record_to_process, table_name, show_progress, drop_empty_columns)

        self.schema = schema
        self.update_table = update_table
        self.psycopg2_connect_string = psycopg2_connect_string

        self.records_to_update = []

        self.copy_stream = BytesIO()
        self.copy_stream.write(pack('>11sii', b'PGCOPY\n\377\r\n\0', 0, 0))

        self.pgepoch = datetime.date(2000, 1, 1)

    def create_enum(self, conn, export_def):
        try:
            with conn.cursor() as cursor:
                cursor.execute('SELECT enum_range(null::"%s")' % export_def.psql_type)
                existing_enum_values = cursor.fetchall()[0][0]

                for (key, value)in export_def.enum.items():
                    key = key.decode()

                    if key not in existing_enum_values and key != b'*':
                        try:
                            conn.set_isolation_level(0)
                            cursor.execute('ALTER TYPE "%s" ADD VALUE \'%s\';' % (export_def.psql_type, key))
                            conn.commit()
                            conn.set_isolation_level(1)

                            self.logging.debug("added '%s' to enum '%s'" % (key, export_def.psql_type))
                        except Exception as e:
                            conn.rollback()

                            self.logging.error("could add value '%s' to enum '%s'\n\t%s" % (key, export_def.psql_type, e))

                            return False

        except Exception as e:
            conn.rollback()

            try:
                with conn.cursor() as cursor:
                    cursor.execute('CREATE TYPE "%s" AS ENUM(%s)' % (export_def.psql_type, (", ".join("'%s'" % key.decode() for (key, value) in export_def.enum.items()))))
                    conn.commit()

                    self.logging.debug("created enum '%s'" % export_def.psql_type)
            except Exception as e:
                conn.rollback()

                self.logging.error("could not create enum '%s'\n\t%s" % (export_def.psql_type, e))

                return False

        with conn.cursor() as cursor:
            try:
                cursor.execute("""SELECT pg_type.oid FROM pg_type INNER JOIN pg_namespace ON pg_namespace.oid = pg_type.typnamespace
                                  WHERE pg_namespace.nspname = %s AND pg_type.typname = %s;""", (self.schema, export_def.psql_type))

                return FieldExportDefinition(export_def.field_id, export_def.field, export_def.type,
                                             export_def.psql_type, export_def.psql_cast, cursor.fetchone()[0],
                                             export_def.is_array, export_def.split, export_def.subscript,
                                             export_def.is_enum, export_def.enum, export_def.pos)
            except Exception as e:
                conn.rollback()

                self.logging.error("could not determine oid of '%s'" % export_def.psql_type)

                return False

    def create_table(self, conn):
        try:
            pgsql_fields = [' "fm_id" bigint']

            for export_def in self.export_definition.values():
                if export_def.is_array and export_def.is_enum:
                    pgsql_fields.append(' "%s" "%s"[]' % (export_def.field.label, export_def.psql_type))
                elif export_def.is_enum:
                    pgsql_fields.append(' "%s" "%s"' % (export_def.field.label, export_def.psql_type))
                elif export_def.is_array:
                    pgsql_fields.append(' "%s" %s[]' % (export_def.field.label, export_def.psql_type))
                else:
                    pgsql_fields.append(' "%s" %s' % (export_def.field.label, export_def.psql_type))

            pgsql_fields.append(' "fm_mod_id" bigint')
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

        for export_def in list(self.export_definition.values()):
            if export_def.is_enum and export_def.psql_type not in handeled_enums:
                new_export_def = self.create_enum(conn, export_def)

                if not new_export_def:
                    return False

                self.export_definition[export_def.field_id] = new_export_def
                handeled_enums.add(new_export_def.psql_type)

        if not self.update_table:
            if not self.create_table(conn):
                return False
        else:
            with conn.cursor() as cursor:
                cursor.execute("""SELECT column_name
                                  FROM information_schema.columns c
                                  LEFT JOIN information_schema.element_types e
                                  ON ((c.table_catalog, c.table_schema, c.table_name, 'TABLE', c.dtd_identifier) =
                                  (e.object_catalog, e.object_schema, e.object_name, e.object_type, e.collection_type_identifier))
                                  WHERE table_schema = %s AND table_name = %s
                                  ORDER BY c.ordinal_position;""", (self.schema, self.table_name))

                preset_columns = cursor.fetchall()

                field_labels_to_dump = [export_def.field.label for export_def in self.export_definition.values()]

                for (preset_column_label, ) in preset_columns:
                    if preset_column_label == 'fm_id' or preset_column_label == 'fm_mod_id':
                        continue

                    if preset_column_label not in field_labels_to_dump:
                        self.logging.debug("deleting column '%s'" % preset_column_label)
                        cursor.execute('ALTER TABLE "%s" DROP COLUMN "%s";' % (self.table_name, preset_column_label))

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

            cursor.execute('CREATE TEMP TABLE "%s" ("fm_id" bigint PRIMARY KEY);' % temp_table_name)

            copy_stream = BytesIO()
            copy_stream.write(pack('>11sii', b'PGCOPY\n\377\r\n\0', 0, 0))

            for fm_id in self.fp5file.records_index:
                copy_stream.write(struct.pack('>hiq', 1, 8, fm_id))

            copy_stream.write(pack('!h', -1))
            copy_stream.seek(0)

            cursor.copy_expert('COPY "%s" FROM STDIN WITH BINARY' % (temp_table_name), copy_stream)

            cursor.execute(
                """DELETE FROM "%s"."%s" WHERE "%s".fm_id IN (
                    SELECT fm_id FROM "%s"."%s"
                    LEFT JOIN "%s" USING(fm_id)
                    WHERE "%s".fm_id IS NULL
                );""" % (
                    self.schema, self.table_name, self.table_name,
                    self.schema, self.table_name,
                    temp_table_name,
                    temp_table_name
                )
            )

            self.deleted_records = cursor.rowcount

            conn.commit()

        self.logging.info("records deleted")

    def pre_run_actions(self, conn):
        self.set_locale()

        psycopg2.extras.register_uuid()

        if self.table_name is None:
            self.table_name = self.fp5file.db_name

        self.create_schema(conn)

        if not self.create_table_and_enums(conn):
            return False

        self.start_time = self.eta_last_updated = time.time()

        self.records_to_process_count = self.fp5file.records_count

        if self.first_record_to_process is not None:
            self.records_to_process_count -= self.fp5file.records_index.index(self.first_record_to_process)

        if self.update_table:
            self.delete_records(conn)

        return True

    def flush_batch(self, conn):
        with conn.cursor() as cursor:
            self.copy_stream.write(pack('!h', -1))
            self.copy_stream.seek(0)

            if self.records_to_update:
                cursor.execute('DELETE FROM "%s"."%s" WHERE fm_id IN %%s;' % (self.schema, self.table_name), (tuple(self.records_to_update), ))

                self.records_to_update.clear()

            try:

                columns = ['"fm_id"'] + ['"%s"' % export_def.field.label for export_def in self.export_definition.values()] + ['"fm_mod_id"']
                cursor.copy_expert('COPY "%s"."%s" (%s) FROM STDIN WITH BINARY' % (self.schema, self.table_name, ", ".join(columns)), self.copy_stream)
            except psycopg2.DataError as error:
                self.logging.error(self.table_name)
                self.logging.error(error)

            self.copy_stream.close()

    def run(self):
        try:
            with psycopg2.connect(self.psycopg2_connect_string) as conn:
                with conn.cursor() as cursor:
                    if not self.pre_run_actions(conn):
                        return False

                    field_count = len(self.export_definition) + 2

                    cursor.execute("""PREPARE get_mod_id AS SELECT "fm_mod_id" FROM "%s"."%s" WHERE "fm_id" = $1;""" % (self.schema, self.table_name))

                    if self.first_record_to_process is not None:
                        start_node_path = b'\x05/' + encode_vli(self.first_record_to_process)
                    else:
                        start_node_path = None

                    table_fields_present = set()
                    token_ids_to_return = set(self.export_definition.keys())

                    for (record_id_bin, record_tokens) in self.fp5file.data.sub_nodes(b'\x05', start_node_path=start_node_path, token_ids_to_return=token_ids_to_return):
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

                        self.copy_stream.write(pack('>HIq', field_count, 8, record_id))
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
                                    stream_pos_before = self.copy_stream.tell()
                                    self.copy_stream.write(b'\xfe\xfe\xfe\xfe')

                                    self.copy_stream.write(pack('>IIIII', 1, 1 if None in value else 0, export_def.pg_oid, len(value), 1))

                                    for sub_value in value:
                                        if sub_value is None or (sub_value == b'' and export_def.split):
                                            self.copy_stream.write(b'\xff\xff\xff\xff')
                                        else:
                                            if not self.values_for_field_type(sub_value, export_def):
                                                had_errors = True
                                                self.copy_stream.write(b'\xff\xff\xff\xff')
                                                self.aggregate_errors(export_def, record_id, sub_value)

                                    stream_pos_end = self.copy_stream.tell()

                                    self.copy_stream.seek(stream_pos_before)
                                    self.copy_stream.write(pack('>I', stream_pos_end - stream_pos_before - 4))
                                    self.copy_stream.seek(stream_pos_end)
                                else:
                                    if not self.values_for_field_type(value, export_def):
                                        had_errors = True
                                        self.copy_stream.write(b'\xff\xff\xff\xff')
                                        self.aggregate_errors(field_id_bin, record_id, values[field_id_bin])
                            else:
                                self.copy_stream.write(b'\xff\xff\xff\xff')

                        if had_errors:
                            self.copy_stream.write(pack('>Iq', 8, -1))
                        else:
                            self.copy_stream.write(pack('>Iq', 8, mod_id))

                        if update_record:
                            self.updated_records += 1
                            self.records_to_update.append(record_id)
                        else:
                            self.inserted_records += 1

                        # flush
                        if self.copy_stream.tell() >= 10485760:
                            self.flush_batch(conn)

                            self.copy_stream = BytesIO()
                            self.copy_stream.write(pack('>11sii', b'PGCOPY\n\377\r\n\0', 0, 0))

                    # final flush
                    if self.copy_stream.tell() > 19:
                        self.flush_batch(conn)

                    # drop empty column
                    if self.drop_empty_columns and not self.update_table:
                        with conn.cursor() as cursor:
                            for field_id_bin, export_def in self.export_definition.items():
                                if field_id_bin not in table_fields_present:
                                    cursor.execute('ALTER TABLE "%s" DROP COLUMN  "%s";\n' % (self.table_name, export_def.field.label))

                    # deallocate prepares statement
                    with conn.cursor() as cursor:
                        cursor.execute("""DEALLOCATE get_mod_id;""")

                    conn.commit()

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
            print("inserted %d" % self.inserted_records)
        else:
            print("inserted %d / updated %d / deleted %d / processed %d" % (self.inserted_records, self.updated_records, self.deleted_records, self.processed_records))

        sys.stdout.flush()

    def values_for_field_type(self, value, export_def):
        try:
            if type(value) is OrderedDict and b'\x01' in value and b'\xff\x00' in value:
                value = value[b'\x01']

            if value is None:
                self.copy_stream.write(b'\xff\xff\xff\xff')

                return True

            # text
            elif export_def.pg_oid == 0x19:
                value = value.replace(b'\x00', b'').decode(self.fp5file.encoding).encode()

                self.copy_stream.write(pack('>I', len(value)))
                self.copy_stream.write(value)

                return True

            # integer
            elif export_def.pg_oid == 0x17:
                self.copy_stream.write(pack('>Ii', 4, int(value)))

                return True

            # biginteger
            elif export_def.pg_oid == 0x14:
                self.copy_stream.write(pack('>Iq', 8, int(value)))

                return True

            # numeric
            elif export_def.pg_oid == 0x06A4:
                value_bin = self.numeric_string_to_postgres_numeric_bytes(value)

                if value_bin:
                    self.copy_stream.write(value_bin)

                    return True
                else:
                    return False

            # date
            elif export_def.pg_oid == 0x043A:
                date, check = self.ptd_parser.parseDT(value.decode())

                if not check:
                    return False

                self.copy_stream.write(pack('>Ii', 4, (date.date() - self.pgepoch).days))

                return True

            # time
            elif export_def.pg_oid == 0x043B:
                match = re.match('\s*(\d?\d):(\d?\d)(:\d?\d)?\s*', value.decode()).groups()

                if match:
                    if len(match) == 3:
                        seconds = (int(match[0]) * 3600 + int(match[1]) * 60 + int(match[2][1:])) * 1000000

                        self.copy_stream.write(pack('>Iq', 8, seconds))

                        return True
                    elif len(match) == 2:
                        seconds = (int(match[0]) * 3600 + int(match[1]) * 60) * 1000000

                        self.copy_stream.write(pack('>Iq', 8, seconds))

                        return True

                return False

            # uuid
            elif export_def.pg_oid == 0x0B86:
                value = uuid.UUID(value.decode())

                self.copy_stream.write(b'\x00\x00\x00\x10')
                self.copy_stream.write(value.bytes)

                return True

            # boolean
            elif export_def.pg_oid == 0x0010:
                if value.lower() in (b'ja', b'yes', b'true', b'1', b'ok'):
                    self.copy_stream.write(b'\x00\x00\x00\x01\x01')

                    return True
                elif value.lower() in (b'nein', b'no', b'false', b'0', b''):
                    self.copy_stream.write(b'\x00\x00\x00\x01\x00')

                    return True

            # enum
            elif export_def.is_enum:
                value = value.upper()

                for enum_key, enum_value in export_def.enum.items():
                    if b'*' != enum_key and value in enum_value:
                        if enum_key == b'NULL':
                            self.copy_stream.write(b'\xff\xff\xff\xff')
                        else:
                            self.copy_stream.write(pack('>I', len(enum_key)))
                            self.copy_stream.write(enum_key)

                        return True
                else:
                    if b'*' in export_def.enum:
                        catch_all = export_def.enum[b'*']

                        if catch_all is None or catch_all == b'NULL':
                            self.copy_stream.write(b'\xff\xff\xff\xff')
                        else:
                            self.copy_stream.write(pack('>I', len(catch_all)))
                            self.copy_stream.write(catch_all)

                        return True
        except err:
            return False

        return False

    def numeric_string_to_postgres_numeric_bytes(self, numeric_string):
        sign = 0x0000

        found_dp = False
        found_digits = False

        dweight = -1
        dscale = 0

        ddigits = 4
        decdigits = bytearray(len(numeric_string) + 8)

        for char in numeric_string:
            if not found_digits:
                if not found_dp:
                    if char == self.decimal_point_char:
                        found_dp = True
                        found_digits = True
                    elif char == 0x2D:  # '-'
                        sign = 0x4000
                    elif char == 0x2B or char == 0x20 or char == 0x09 or char == 0x30:  # '+' or ' ' or '\t' or '0'
                        pass
                    elif 0x30 < char <= 0x39:
                        found_digits = True
                        decdigits[ddigits] = char - 0x30
                        ddigits += 1

                        if found_dp:
                            dscale += 1
                        else:
                            dweight += 1
                    else:
                        return None

            elif not found_dp:
                if char == self.decimal_point_char:
                    found_dp = True
                elif 0x30 <= char <= 0x39:
                      found_digits = True
                      decdigits[ddigits] = char - 0x30
                      ddigits += 1

                      if found_dp:
                          dscale += 1
                      else:
                          dweight += 1
                elif char == self.thousands_separator_char:
                    pass
                else:
                    break
            else:
               if 48 <= char <= 59:
                   decdigits[ddigits] = char - 0x30
                   ddigits += 1

                   if found_dp:
                       dscale += 1
                   else:
                       dweight += 1
               else:
                   break

        ddigits -= 4

        if dweight >= 0:
            weight = (dweight + 4) // 4 - 1
        else:
            weight = -((-dweight - 1) // 4 + 1)

        offset = (weight + 1) * 4 - (dweight + 1)
        ndigits = (ddigits + offset + 4 - 1) // 4

        bytes_needed = 8 + ndigits * 2
        numeric_binary = bytearray(bytes_needed)
        numeric_binary[0:8] = struct.pack('>IHhHH', bytes_needed, ndigits, weight, sign, dscale)

        i = 4 - offset
        j = 12

        while ndigits > 0:
            ndigits -= 1
            numeric_binary[j:j + 2] = struct.pack('>H', ((decdigits[i] * 10 + decdigits[i + 1]) * 10 + decdigits[i + 2]) * 10 + decdigits[i + 3])
            i += 4
            j += 2

        return numeric_binary
