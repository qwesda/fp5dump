#!/usr/bin/env python3

import re
import os
import argparse
import logging

from .fp5file.fp5file import FP5File


def list_fields(args):
    with FP5File(args.input.name, encoding=args.encoding) as fp5file:
        print("ID TYPE[REPETITIONS] IS_STORED NAME")

        for field_id in sorted(fp5file.fields.keys()):
            field = fp5file.fields[field_id]

            if not args.show_unstored and not field.stored:
                continue

            if field.typename not in args.include_fields_types:
                continue

            print(field)


def count_records(args):
    with FP5File(args.input.name) as fp5file:
        print(fp5file.records_count)


def dump_blocks(args):
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


def dump_records(args):
    def determine_fields_to_dump(fp5file, args):
        fields = fp5file.fields
        fields_ids_to_include = []

        for field_id in sorted(fields.keys()):
            field = fields[field_id]

            include = False

            if field.stored:
                for include_fields_name in args.include_fields:
                    if include_fields_name == field.label:
                        include = True

                for include_field_reg in args.include_fields_like:
                    if re.search(include_field_reg, field.label):
                        include = True

                if not args.include_fields and not args.include_fields_like:
                    include = True

            if include:
                fields_ids_to_include.append(field_id)

        for field_id in sorted(fields.keys()):
            field = fields[field_id]

            exclude = False

            for ignore_field_name in args.ignore_fields:
                if ignore_field_name == field.label:
                    exclude = True

            for ignore_field_reg in args.ignore_fields_like:
                if re.search(ignore_field_reg, field.label):
                    exclude = True

            if field.typename in args.ignore_field_types:
                exclude = True

            if exclude and field_id in fields_ids_to_include:
                fields_ids_to_include.remove(field_id)

        return fields_ids_to_include

    with FP5File(args.input.name, encoding=args.encoding) as fp5file:
        field_ids_to_dump = determine_fields_to_dump(fp5file, args)

        fp5file.dump_records_pgsql(field_ids_to_dump, filename=args.output, show_progress=args.progress)


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

    list_fields_parser.add_argument('--encoding',
                                    choices=['ascii', 'cp1252', 'latin_1', 'macroman'],
                                    default='latin_1',
                                    help='the encoding to interpret string values')

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
                                     choices=['ascii', 'cp1252', 'latin_1', 'macroman'],
                                     default='latin_1',
                                     help='the encoding to interpret strings')

    dump_records_parser.add_argument('--ignore-fields', nargs='+',
                                     default=[],
                                     help='ignore fields <by field name>')

    dump_records_parser.add_argument('--ignore-fields-like', nargs='+',
                                     default=[],
                                     help='ignore fields with names that match these regular expressions')

    dump_records_parser.add_argument('--ignore-field-types', nargs='*',
                                     default=['GLOBAL', 'CONTAINER'],
                                     choices=['TEXT', 'NUMBER', 'DATE', 'TIME', 'CALC', 'SUMMARY', 'GLOBAL',
                                              'CONTAINER'],
                                     help='show only fields with these types')

    dump_records_parser.add_argument('--include-fields', nargs='+',
                                     default=[],
                                     help='include fields <by field name> in the dump. '
                                          'if no fieldnames are specified all will be exported, '
                                          'if they are not specified as ignored')

    dump_records_parser.add_argument('--include-fields-like', nargs='+',
                                     default=[],
                                     help='include fields with names that match these regular expressions')

    dump_records_parser.add_argument('--output', '-o',
                                     help='the output filename. defaults to `basename(input_file)`.psql')

    dump_records_parser.add_argument('--progress', '-p', action='store_true',
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
        list_fields(args)
    elif args.action == "count-records":
        count_records(args)
    elif args.action == "dump-blocks":
        dump_blocks(args)
    elif args.action == "dump-records":
        dump_records(args)


if __name__ == '__main__':
    main()
