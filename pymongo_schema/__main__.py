#!/usr/bin/env python
# coding: utf8

"""
CLI tool to use pymongo_schema modules.

python -m pymongo_schema --help
"""

import json
import logging
import sys
from argparse import ArgumentParser
from time import time

import pymongo

from pymongo_schema.compare import compare_schemas_bases
from pymongo_schema.export import transform_data_to_file, HtmlOutput, TsvOutput
from pymongo_schema.extract import extract_pymongo_client_schema
from pymongo_schema.filter import filter_mongo_schema_namespaces
from pymongo_schema.tosql import mongo_schema_to_mapping

logger = logging.getLogger()


def add_subparser_extract(subparsers, parent_parsers):
    subparser = subparsers.add_parser('extract', parents=parent_parsers)
    subparser.add_argument('-d', '--databases', nargs='*',
                           help='Only analyze those databases. By default analyze all databases '
                                'in MongoDB instance')
    subparser.add_argument('-c', '--collections', nargs='*',
                           help='Only analyze those collections. By default analyze all '
                                'collections in each database')
    subparser.add_argument('--port', default=27017, type=int,
                           help='Port to connect to MongoDB [default: 27017]')
    subparser.add_argument('--host', default='localhost',
                           help='Server to connect to MongoDB [default: localhost]')


def add_subparser_transform(subparsers, parent_parsers):
    subparser = subparsers.add_parser('transform', parents=parent_parsers)
    subparser.add_argument('input', nargs='?',
                           help='json formatted input file (schema, mapping, ...). '
                                '[default standard input]')
    subparser.add_argument('--category', default='schema',
                           help='category of input (schema, mapping, diff) [default schema]')
    subparser.add_argument('-n', '--filter',
                           help='Config file to read namespace to filter for schema input. '
                                'json format expected.')
    subparser.add_argument('--columns', nargs='+',
                           help='''
                           Columns to get in 'tsv', 'html', 'md' or 'xlsx' format.
                           For schema, columns are to be chosen in :
                               FIELD_FULL_NAME ('.' for subfields, ':' for subfields in arrays)
                               FIELD_COMPACT_NAME (idem, without parent object names)
                               FIELD_NAME
                               DEPTH
                               TYPE
                               COUNT
                               PROP_IN_OBJECT
                               PERCENTAGE
                               TYPES_COUNT
                           Columns have to be separated by whitespace, and are case insensitive.
                           Default for 'html' and 'md' output is {}
                           Default for 'tsv' and 'xlsx' output is {}'''.format(
                               HtmlOutput.get_default_columns()['schema'],
                               TsvOutput.get_default_columns()['schema']))
    subparser.add_argument('--without-counts', action='store_true',
                           help='Remove counts information from json and yaml outputs')


def add_subparser_tosql(subparsers, parent_parsers):
    subparser = subparsers.add_parser('tosql', parents=parent_parsers)
    subparser.add_argument('input', nargs='?',
                           help='Input schema file to map to sql (json format). '
                                'Default to standard input')


def add_subparser_compare(subparsers, parent_parsers):
    subparser = subparsers.add_parser('compare', parents=parent_parsers)
    subparser.add_argument('input',
                           help='Input schema')
    subparser.add_argument('expected', nargs='?',
                           help='Expected schema')


def main(argv=None):
    """ Launch pymongo_schema (assuming CLI).

    :param argv: command line arguments to pass directly to docopt.
            Useful for usage from another python program.
    """
    parent_parser = ArgumentParser(add_help=False)
    parent_parser.add_argument('-f', '--formats', nargs='*', default=['json'],
                               help="List Output formats:  "
                                    "'tsv', 'xlsx', 'yaml', 'html', 'md' or 'json'"
                                    "Multiple format may be specified. [default: json]")
    parent_parser.add_argument('-o', '--output',
                               help='Output file. Default to standard output. Extension added '
                                    'automatically if omitted (useful for multi-format outputs)')
    parser = ArgumentParser("extract schemas from MongoDB")
    parser.add_argument('--quiet', action='store_true',
                        help='Remove logging on standard output')
    subparsers = parser.add_subparsers(dest='command')

    add_subparser_extract(subparsers, [parent_parser])
    add_subparser_transform(subparsers, [parent_parser])
    add_subparser_tosql(subparsers, [parent_parser])
    add_subparser_compare(subparsers, [parent_parser])

    args = parser.parse_args(argv)

    # Parse command line argument
    preprocess_arg(args)

    # Extract mongo schema
    if args.command == 'extract':
        output_dict = extract_schema(args)

    # Transform mongo schema
    if args.command == 'transform':
        output_dict = transform_schema(args)

    # Map mongo schema to sql
    if args.command == 'tosql':
        output_dict = schema_to_sql(args)
        args.category = 'mapping'

    # Compare two schemas
    if args.command == 'compare':
        output_dict = compare_schemas(args)
        args.category = 'diff'

    # Output dict
    logger.info('=== Write output')
    if output_dict:
        transform_data_to_file(output_dict, **vars(args))
    else:
        logger.warn("WARNING : output is empty, we do not write any file.")


def preprocess_arg(arg):
    """ Preprocess arguments from command line."""
    if arg.output is None and 'xlsx' in arg.formats:
        logger.warn("WARNING : xlsx format is not supported on standard output. "
                    "Switching to tsv output.")
        arg.formats.remove('xlsx')
        arg.formats.append('tsv')


def initialize_logger(arg):
    """ Initialize logging to standard output, if not quiet."""
    if not arg.quiet:
        stream_handler = logging.StreamHandler()
        logger.addHandler(stream_handler)


def extract_schema(arg):
    """ Main entry point function to extract schema.

    :param arg:
    :return mongo_schema: dict
    """
    start_time = time()
    logger.info('=== Start MongoDB schema analysis')
    client = pymongo.MongoClient(host=arg.host, port=arg.port)

    mongo_schema = extract_pymongo_client_schema(client,
                                                 database_names=arg.databases,
                                                 collection_names=arg.collections)

    logger.info('--- MongoDB schema analysis took %.2f s', time() - start_time)
    return mongo_schema


def transform_schema(arg):
    """ Main entry point function to transform a schema.

    :param arg: dict
    :return filtered_mongo_schema: dict
    """
    logger.info('=== Transform existing mongo schema (filter, new format, and/or select infos)')
    input_schema = load_input_schema(arg)
    if arg.filter is not None:
        with open(arg.filter, 'r') as f:
            config = json.load(f)
        output_schema = filter_mongo_schema_namespaces(input_schema, config['namespaces'])
    else:
        output_schema = input_schema
    return output_schema


def schema_to_sql(arg):
    """ Main entry point function to generate a mapping from mongo to sql.

    :param arg: dict
    :return mongo_to_sql_mapping: dict
    """
    logger.info('=== Generate mapping from mongo to sql')
    input_schema = load_input_schema(arg)
    mongo_to_sql_mapping = mongo_schema_to_mapping(input_schema)
    return mongo_to_sql_mapping


def compare_schemas(arg):
    """

    :param arg:
    :return:
    """
    logger.info('=== Compare schemas')
    input_schema = load_input_schema(arg)
    expected_schema = load_input_schema(arg, opt='expected')
    diff = compare_schemas_bases(input_schema, expected_schema)
    return diff


def load_input_schema(arg, opt='input'):
    """Load schema from file or stdin."""
    try:
        filename = getattr(arg, opt)
    except AttributeError:
        input_schema = json.load(sys.stdin)
    else:
        with open(filename, 'r') as f:
            input_schema = json.load(f)

    return input_schema


if __name__ == '__main__':
    main()
