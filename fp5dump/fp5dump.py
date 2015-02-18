#!/usr/bin/env python3
from collections import OrderedDict

import re
import os
import argparse
import logging
import psycopg2

try:
    from fp5file.fp5file import FP5File, FieldExportDefinition
    from fp5file.block import encode_vli, decode_vli
except ImportError:
    from .fp5file.fp5file import FP5File, FieldExportDefinition
    from .fp5file.block import encode_vli, decode_vli


def __list_fields__(args):
    with FP5File(args.input.name, encoding=args.encoding) as fp5file:
        print("ID TYPE[REPETITIONS] IS_STORED NAME")

        for field_id in sorted(fp5file.fields.keys()):
            field = fp5file.fields[field_id]

            if not args.show_unstored and not field.stored:
                continue

            if field.typename not in args.include_fields_types:
                continue

            print(field)


def __count_records__(args):
    with FP5File(args.input.name) as fp5file:
        print(fp5file.records_count)


def __dump_blocks__(args):
    if 'index' in args.type:
        with FP5File(args.input.name) as fp5file:
            fp5file.dump_index_blocks(args.output)
    elif 'data' in args.type:
        if args.with_path:
            if re.match("^(([0-9a-fA-F]{2})+/)?([0-9a-fA-F]{2})+$", args.with_path):
                with FP5File(args.input.name) as fp5file:
                    fp5file.dump_blocks_with_path(args.with_path.encode("ascii"), args.output)
            else:
                print("path '%s' is invalid, should look like: '05', '03/02', '04/05/03'" % (args.with_path))
        else:
            with FP5File(args.input.name) as fp5file:
                fp5file.dump_data_blocks(args.output)


def __dump_records__(args):
    with FP5File(args.input.name, encoding=args.encoding) as fp5file:
        if not args.definition:
            fields_to_dump = fp5file.generate_export_definition(include_fields=args.include_fields,
                                                                include_fields_like=args.include_fields_like,
                                                                ignore_fields=args.ignore_fields,
                                                                ignore_fields_like=args.ignore_fields_like,
                                                                ignore_field_types=args.ignore_field_types,
                                                                treat_all_as_string=args.assume_string,
                                                                use_locale=args.locale,
                                                                encoding=args.encoding)
        else:
            fields_to_dump = fp5file.load_export_definition(args.definition)


        if fields_to_dump is None:
            logging.error("no fields to dump")
            return

        if fp5file.records_count == 0:
            logging.error("no records to dump")
            return

        fp5file.dump_records_pgsql(fields_to_dump,
                                   filename=args.output,
                                   drop_empty_columns=args.drop_empty_columns,
                                   show_progress=args.progress)


def __insert_records__(args):
    with FP5File(args.input.name, encoding=args.encoding) as fp5file:
        if not args.definition:
            fields_to_dump = fp5file.generate_export_definition(include_fields=args.include_fields,
                                                                include_fields_like=args.include_fields_like,
                                                                ignore_fields=args.ignore_fields,
                                                                ignore_fields_like=args.ignore_fields_like,
                                                                ignore_field_types=args.ignore_field_types,
                                                                treat_all_as_string=args.assume_string,
                                                                use_locale=args.locale,
                                                                encoding=args.encoding)
        else:
            fields_to_dump = fp5file.load_export_definition(args.definition)

        if not fields_to_dump:
            logging.error("no fields to dump")
            return

        if fp5file.records_count == 0:
            logging.error("no records to dump")
            return

        if args.schema is not None:
            fp5file.insert_records_into_postgres(fields_to_dump,
                                                 psycopg2_connect_string=args.pg,
                                                 schema=args.schema,
                                                 drop_empty_columns=args.drop_empty_columns,
                                                 show_progress=args.progress)
        else:
            logging.error("a schema has to be specified if records should be inserted into a db")


def __update_records_determine_action__(fp5file, fields_to_dump, psycopg2_connect_string, schema, truncate_last_rows):
    if fields_to_dump is None:
        logging.error("no fields to dump")
        return

    if fp5file.records_count == 0:
        logging.error("no records to dump")
        return

    with psycopg2.connect(psycopg2_connect_string) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT schema_name FROM information_schema.schemata;")

            schemata = set(schema[0] for schema in cursor.fetchall())

            if schema not in schemata:
                return ('full', None)
            else:
                cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s;", (schema,))

                tables = set(table[0] for table in cursor.fetchall())

                if fp5file.db_name not in tables:
                    return ('full', None)
                else:
                    cursor.execute("""SELECT column_name,
                                      CASE
                                        WHEN c.data_type = 'ARRAY' AND e.data_type = 'USER-DEFINED' THEN '"' || e.udt_name || '"[]'
                                        WHEN c.data_type = 'ARRAY' THEN e.data_type || '[]'
                                        WHEN c.data_type = 'USER-DEFINED' THEN '"' || c.udt_name || '"'
                                        ELSE c.data_type
                                      END AS element_type
                                      FROM information_schema.columns c
                                      LEFT JOIN information_schema.element_types e
                                      ON ((c.table_catalog, c.table_schema, c.table_name, 'TABLE', c.dtd_identifier) =
                                      (e.object_catalog, e.object_schema, e.object_name, e.object_type, e.collection_type_identifier))
                                      WHERE table_schema = %s AND table_name = %s;""", (schema, fp5file.db_name))

                    column_infos = set(column_info for column_info in cursor.fetchall())

                    normalized_dest_column_infos = [('fm_id', 'bigint'), ('fm_mod_id', 'bigint')]
                    for (field_id, field_def) in fields_to_dump.items():
                        normalized_dest_column_infos.append((fp5file.fields[field_id].label, field_def.psql_cast[2:] if field_def.is_array or field_def.is_enum else field_def.psql_type))

                    if set(column_infos) != set(normalized_dest_column_infos):
                        logging.info("the table to be updated has a different set of columns then the requested export definition")

                        return ('full', None)

                    if truncate_last_rows > 0:
                        cursor.execute("""DELETE FROM "%s"."%s" WHERE fm_id > (SELECT fm_id FROM "%s"."%s" ORDER BY fm_id DESC LIMIT 1 OFFSET %d)""" %
                                (schema, fp5file.db_name, schema, fp5file.db_name, truncate_last_rows))

                    cursor.execute("""SELECT fm_id FROM "%s"."%s" ORDER BY fm_id DESC""" %
                            (schema, fp5file.db_name))

                    while True:
                        id = cursor.fetchone()

                        if id is None:
                            return ('use_existing_table', None)

                        try:
                            id_encoded = encode_vli(id[0])
                            index = fp5file.records_index.index(id_encoded)

                            if index + 1 < fp5file.records_count:
                                first_record_to_export = fp5file.records_index[index+1]

                                return ('partial', first_record_to_export)
                            else:
                                logging.info("nothing to do – latest fm_id in db and fp5 file are the same")
                                return (None, None)
                        except ValueError:
                            continue

def __update_records__(args):
    with FP5File(args.input.name, encoding=args.encoding) as fp5file:
        if not args.definition:
            fields_to_dump = fp5file.generate_export_definition(include_fields=args.include_fields,
                                                                include_fields_like=args.include_fields_like,
                                                                ignore_fields=args.ignore_fields,
                                                                ignore_fields_like=args.ignore_fields_like,
                                                                ignore_field_types=args.ignore_field_types,
                                                                treat_all_as_string=args.assume_string,
                                                                use_locale=args.locale,
                                                                encoding=args.encoding)
        else:
            fields_to_dump = fp5file.load_export_definition(args.definition)

        if fields_to_dump is None:
            logging.error("no fields to dump")
            return

        if fp5file.records_count == 0:
            logging.error("no records to dump")
            return

        action, first_record_to_export = __update_records_determine_action__(fp5file, fields_to_dump, args.pg, args.schema, args.truncate_last_rows)

        if action == 'full':
            fp5file.insert_records_into_postgres(fields_to_dump,
                                                 psycopg2_connect_string=args.pg,
                                                 schema=args.schema,
                                                 show_progress=args.progress)
        elif action == 'use_existing_table':
            fp5file.update_records_into_postgres(fields_to_dump,
                                                 psycopg2_connect_string=args.pg,
                                                 schema=args.schema,
                                                 show_progress=args.progress)
        elif action == 'partial':
            fp5file.update_records_into_postgres(fields_to_dump,
                                                 psycopg2_connect_string=args.pg,
                                                 schema=args.schema,
                                                 first_record_to_export=first_record_to_export,
                                                 show_progress=args.progress)


def main():
    version = None

    with open(os.path.join(os.path.dirname(__file__), '__init__.py'), 'r') as f:
        version = re.search('__version__\s*=\s*\'\s*(\d+\.\d+\.\d+)\s*\'', f.read()).group(1)

    main_parser = argparse.ArgumentParser(prog='Fp5Dump',
                                          description='dumps the content of FileMaker 5/6 .fp5 files to psql')
    main_parser.add_argument('--version', action='version', version=version)
    main_parser.add_argument('-v', '--verbosity', default=0, action='count',
                             help='sets the verbosity level. -v = info -vv = debug')

    sub_parsers = main_parser.add_subparsers(dest='action')


    # list-fields
    list_fields_parser = sub_parsers.add_parser('list-fields',
                                                help='lists the field and their type of a fp5 file')

    list_fields_parser.add_argument('input', type=argparse.FileType('r'),
                                    help='the fp5 file to list the fields from')

    list_fields_parser.add_argument('--encoding', nargs='?', default=None,
                                       help='the encoding to interpret strings defaults to "latin_1"')

    list_fields_parser.add_argument('--show-unstored', action='store_true',
                                    help='show only unstored fields')

    list_fields_parser.add_argument('--include-fields-types', nargs='*',
                                    choices=['TEXT', 'NUMBER', 'DATE', 'TIME', 'CALC', 'SUMMARY', 'GLOBAL',
                                             'CONTAINER'],
                                    default=['TEXT', 'NUMBER', 'DATE', 'TIME', 'CALC', 'SUMMARY', 'GLOBAL',
                                             'CONTAINER'],
                                    help='show only fields with these types')


    # count-records
    count_records_parser = sub_parsers.add_parser('count-records',
                                                  help='counts the records of a fp5 file')

    count_records_parser.add_argument('input', type=argparse.FileType('r'),
                                      help='the fp5 file to count the records of')


    # dump-blocks
    dump_blocks_parser = sub_parsers.add_parser('dump-blocks',
                                                help='dumps the ordered data or index blocks of a fp5 file')

    dump_blocks_parser.add_argument('input', type=argparse.FileType('r'),
                                    help='the fp5 file to dump the data blocks of')

    dump_blocks_parser.add_argument('--output', '-o',
                                    help='the output filename. defaults to `basename(input_file)`.[index|data]')

    dump_blocks_parser.add_argument('--type',
                                    choices=['data', 'index'],
                                    default=['data'],
                                    help='dump the blocks belonging to the index or the data part of the file')

    dump_blocks_parser.add_argument('--with-path',
                                    help='dumps only data block containing nodes of a certain path. e.g. \'03/01\'')


    # dump-records
    dump_records_parser = sub_parsers.add_parser('dump-records',
                                                 help='dump the records of fp5 file')

    dump_records_parser.add_argument('input', type=argparse.FileType('r'),
                                     help='the fp5 file to dump the records of')

    dump_records_parser.add_argument('--encoding',
                                     default='latin_1',
                                     help='the encoding to interpret strings')

    dump_records_parser.add_argument('--locale', nargs='?',
                                     default='en_US',
                                     help='the locale used to interpret date, time and numeric fields')

    dump_records_parser.add_argument('--ignore-fields', nargs='+',
                                     default=[],
                                     help='ignore fields <by field name>')

    dump_records_parser.add_argument('--ignore-fields-like', nargs='+',
                                     default=[],
                                     help='ignore fields with names that match these regular expressions')

    dump_records_parser.add_argument('--ignore-field-types', nargs='*',
                                     default=['GLOBAL', 'CONTAINER'],
                                     choices=['TEXT', 'NUMBER', 'DATE', 'TIME', 'CALC', 'SUMMARY', 'GLOBAL', 'CONTAINER'],
                                     help='show only fields with these types')

    dump_records_parser.add_argument('--include-fields', nargs='+',
                                     default=[],
                                     help='include fields <by field name> in the dump. '
                                          'if no fieldnames are specified all will be exported, '
                                          'if they are not specified as ignored')

    dump_records_parser.add_argument('--assume-string', action='store_true',
                                     help="map all field to string")

    dump_records_parser.add_argument('--include-fields-like', nargs='+',
                                     default=[],
                                     help='include fields with names that match these regular expressions')

    dump_records_parser.add_argument('--output', '-o',
                                     help='the output filename. defaults to `basename(input_file)`.psql')

    dump_records_parser.add_argument('--definition', nargs='?', default=None,
                                     help='a yaml file containing information about what fields should be '
                                          'exported and to which types they should be mapped')

    dump_records_parser.add_argument('--drop-empty-columns', action='store_true',
                                     help='drop columns that have only NULL values')

    dump_records_parser.add_argument('--progress', '-p', action='store_true',
                                     help='show progress while dumping records')


    # insert-records
    insert_records_parser = sub_parsers.add_parser('insert-records',
                                                   help='inserts the records of fp5 file directly into a postgres db')

    insert_records_parser.add_argument('input', type=argparse.FileType('r'),
                                       help='the fp5 file to dump the records of')

    insert_records_parser.add_argument('--encoding', nargs='?', default=None,
                                       help='the encoding to interpret strings defaults to "latin_1"')

    insert_records_parser.add_argument('--locale', nargs='?',
                                       default='en_US',
                                       help='the locale used to interpret date, time and numeric fields')

    insert_records_parser.add_argument('--ignore-fields', nargs='+',
                                       default=[],
                                       help='ignore fields <by field name>')

    insert_records_parser.add_argument('--ignore-fields-like', nargs='+',
                                       default=[],
                                       help='ignore fields with names that match these regular expressions')

    insert_records_parser.add_argument('--ignore-field-types', nargs='*',
                                       default=['GLOBAL', 'CONTAINER'],
                                       choices=['TEXT', 'NUMBER', 'DATE', 'TIME', 'CALC', 'SUMMARY', 'GLOBAL',
                                                'CONTAINER'],
                                       help='show only fields with these types')

    insert_records_parser.add_argument('--include-fields', nargs='+',
                                       default=[],
                                       help='include fields <by field name> in the dump. '
                                            'if no fieldnames are specified all will be exported, '
                                            'if they are not specified as ignored')

    insert_records_parser.add_argument('--assume-string', action='store_true',
                                       help="map all field to string")

    insert_records_parser.add_argument('--include-fields-like', nargs='+',
                                       default=[],
                                       help='include fields with names that match these regular expressions')

    insert_records_parser.add_argument('--schema',
                                       help='the schema the table will be created in - '
                                            'will be created if it does not already exist')

    insert_records_parser.add_argument('--definition', nargs='?', default=None,
                                       help='a yaml file containing information about what fields should be '
                                            'exported and to which types they should be mapped')

    insert_records_parser.add_argument('--pg',
                                       help='the postgres connection string')


    insert_records_parser.add_argument('--drop-empty-columns', action='store_true',
                                       help='drop columns that have only NULL values')

    insert_records_parser.add_argument('--progress', '-p', action='store_true',
                                       help='show progress while dumping records')

    # update-last-records
    update_records_parser = sub_parsers.add_parser('update-records',
                                                   help='updates an existing table by getting the last record id in '
                                                        'a table and only inserts records from the file with bigger'
                                                        'record ids')

    update_records_parser.add_argument('input', type=argparse.FileType('r'),
                                       help='the fp5 file to dump the records of')

    update_records_parser.add_argument('--truncate-last-rows', default=0, type=int,
                                       help='removes the last n records from the db before inserting new ones\n'
                                            'useful for making sure these last records are updated')

    update_records_parser.add_argument('--encoding', nargs='?', default=None,
                                       help='the encoding to interpret strings defaults to "latin_1"')

    update_records_parser.add_argument('--locale', nargs='?',
                                       default='en_US',
                                       help='the locale used to interpret date, time and numeric fields')

    update_records_parser.add_argument('--ignore-fields', nargs='+',
                                       default=[],
                                       help='ignore fields <by field name>')

    update_records_parser.add_argument('--ignore-fields-like', nargs='+',
                                       default=[],
                                       help='ignore fields with names that match these regular expressions')

    update_records_parser.add_argument('--ignore-field-types', nargs='*',
                                       default=['GLOBAL', 'CONTAINER'],
                                       choices=['TEXT', 'NUMBER', 'DATE', 'TIME', 'CALC', 'SUMMARY', 'GLOBAL',
                                                'CONTAINER'],
                                       help='show only fields with these types')

    update_records_parser.add_argument('--include-fields', nargs='+',
                                       default=[],
                                       help='include fields <by field name> in the dump. '
                                            'if no fieldnames are specified all will be exported, '
                                            'if they are not specified as ignored')

    update_records_parser.add_argument('--assume-string', action='store_true',
                                       help="map all field to string – "
                                            "useful if actual data does not correspond to declared data field type")
    update_records_parser.add_argument('--include-fields-like', nargs='+',
                                       default=[],
                                       help='include fields with names that match these regular expressions')

    update_records_parser.add_argument('--schema',
                                       help='the schema the table will be created in - '
                                            'will be created if it does not already exist')

    update_records_parser.add_argument('--definition', nargs='?', default=None,
                                       help='a yaml file containing information about what fields should be '
                                            'exported and to which types they should be mapped')

    update_records_parser.add_argument('--pg',
                                       help='the postgres connection string')

    update_records_parser.add_argument('--progress', '-p', action='store_true',
                                       help='show progress while dumping records')

    args = main_parser.parse_args()

    logger = logging.getLogger('fp5dump')

    logging_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging_handler = logging.StreamHandler()
    logging_handler.setFormatter(logging_formatter)

    if args.verbosity >= 2:
        logger.setLevel(logging.DEBUG)
    elif args.verbosity == 1:
        logger.setLevel(logging.INFO)
    elif args.verbosity == 0:
        logger.setLevel(logging.WARNING)

    logger.addHandler(logging_handler)

    if args.action == "list-fields":
        __list_fields__(args)
    elif args.action == "count-records":
        __count_records__(args)
    elif args.action == "dump-blocks":
        __dump_blocks__(args)
    elif args.action == "dump-records":
        __dump_records__(args)
    elif args.action == "insert-records":
        __insert_records__(args)
    elif args.action == "update-records":
        __update_records__(args)
    else:
        main_parser.print_help()


if __name__ == '__main__':
    main()
