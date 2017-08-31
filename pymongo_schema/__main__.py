#!/usr/bin/env python
# coding: utf8

"""
pymongo-schema extract schemas from MongoDB

Usage:
    pymongo_schema  -h | --help
    pymongo_schema  extract [--database=DB... --collection=COLLECTION... --port=PORT --host=HOST --output=FILENAME --format=FORMAT... --quiet]
    pymongo_schema  transform [--input=FILENAME --filter=FILENAME --output=FILENAME --format=FORMAT... --columns=COLUMNS  --without-counts --quiet]
    pymongo_schema  tosql [--input=FILENAME --output=FILENAME --quiet]
    pymongo_schema  compare [--input=FILENAME --output=FILENAME --format=FORMAT... --expected=FILENAME --category=CATEGORY]

Commands:
    extract                     Extract schema from a MongoDB instance
    transform                   Transform a json schema to another format, potentially filtering or changing columns outputs
    tosql                       Create a mapping from mongo schema to relational schema (json input and output)
    compare                     Compare a mongo schema to another and write differences in the desired format
                                (only the fist difference is noted, not the entire hierarchy)

Options:
    -d --database DB            Only analyze those databases.
                                By default analyze all databases in Mongo instance

    -c --collection COL         Only analyze this collection.
                                Multiple collections may be specified this way.

    --port PORT                 Port to connect to [default: 27017]

    --host HOST                 Server to connect to [default: localhost]

    -i , --input FILENAME       Input schema file, to transform or to map to sql. json format expected.
                                Default to standard input

    -o , --output FILENAME      Output file for schema. Default to standard output.
                                Extension added automatically if omitted (useful for multi-format outputs)

    -e , --expected FILENAME    Expected schema file - json format expected

    -f , --format FORMAT        Output format for schema :  'tsv', 'xlsx', 'yaml', 'html', 'md' or 'json'
                                Multiple format may be specified. [default: json]
                                Note : Output format for mongo to sql mapping is json

    --category FORMAT           Category of input (schema | mapping | diff) [default: schema]

    --columns HEADER            String listing columns to get in 'tsv', 'html', 'md' or 'xlsx' format.
                                Columns are to be chosen in :
                                    FIELD_FULL_NAME         '.' for subfields, ':' for subfields in arrays
                                    FIELD_COMPACT_NAME      idem, without parent object names
                                    FIELD_NAME
                                    DEPTH
                                    TYPE
                                    COUNT
                                    PROP_IN_OBJECT
                                    PERCENTAGE
                                    TYPES_COUNT
                                Columns have to be separated by whitespace, and are case insensitive.
                                Default for 'html' and 'md' output is "Field_compact_name Field_name Count Percentage Types_count"
                                Default for 'tsv' and 'xlsx' output is "Field_full_name Depth Field_name Type"

    -n, --filter FILENAME       Config file to read namespace to filter. json format expected.

    --without-counts            Remove counts information from json and yaml outputs

    --quiet                     Remove logging on standard output

    -h, --help                  show this usage information

"""

import json
import logging
import sys
from time import time

import pymongo
from docopt import docopt

from pymongo_schema.compare import compare_schemas_bases
from pymongo_schema.export import write_output_dict
from pymongo_schema.extract import extract_pymongo_client_schema
from pymongo_schema.filter import filter_mongo_schema_namespaces
from pymongo_schema.tosql import mongo_schema_to_mapping

logger = logging.getLogger()


def main(argv=None):
    """ Launch pymongo_schema (assuming CLI).

    :param argv: command line arguments to pass directly to docopt.
            Useful for usage from another python program.
    """
    # Parse command line argument
    arg = docopt(__doc__, argv=argv, help=True)
    initialize_logger(arg)
    preprocess_arg(arg)

    # Extract mongo schema
    if arg['extract']:
        output_dict = extract_schema(arg)

    # Transform mongo schema
    if arg['transform']:
        output_dict = transform_schema(arg)

    # Map mongo schema to sql
    if arg['tosql']:
        arg['--format'] = ['json']
        output_dict = schema_to_sql(arg)

    # Compare two schemas
    if arg['compare']:
        output_dict = compare_schemas(arg)

    # Output dict
    logger.info('=== Write output')
    if output_dict:
        write_output_dict(output_dict, arg)
    else:
        logger.warn("WARNING : output is empty, we do not write any file.")


def preprocess_arg(arg):
    """ Preprocess arguments from command line."""
    if not arg['--collection']:
        arg['--collection'] = None

    if arg['--output'] is None and 'xlsx' in arg['--format']:
        logger.warn("WARNING : xlsx format is not supported on standard output. "
                    "Switching to tsv output.")
        arg['--format'].remove('xlsx')
        arg['--format'].append('tsv')


def initialize_logger(arg):
    """ Initialize logging to standard output, if not quiet."""
    if not arg['--quiet']:
        stream_handler = logging.StreamHandler()
        logger.addHandler(stream_handler)


def extract_schema(arg):
    """ Main entry point function to extract schema.

    :param arg:
    :return mongo_schema: dict
    """
    start_time = time()
    logger.info('=== Start MongoDB schema analysis')
    client = pymongo.MongoClient(host=arg['--host'], port=int(arg['--port']))

    mongo_schema = extract_pymongo_client_schema(client,
                                                 database_names=arg['--database'],
                                                 collection_names=arg['--collection'])

    logger.info('--- MongoDB schema analysis took %.2f s', time() - start_time)
    return mongo_schema


def transform_schema(arg):
    """ Main entry point function to transform a schema.

    :param arg: dict
    :return filtered_mongo_schema: dict
    """
    logger.info('=== Transform existing mongo schema (filter, new format, and/or select infos)')
    input_schema = load_input_schema(arg)
    namespace = arg['--filter']
    if namespace is not None:
        with open(arg['--filter'], 'r') as f:
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
    expected_schema = load_input_schema(arg, opt='--expected')
    diff = compare_schemas_bases(input_schema, expected_schema)
    return diff


def load_input_schema(arg, opt='--input'):
    """Load schema from file or stdin."""
    if arg[opt] is None:
        input_schema = json.load(sys.stdin)
    else:
        with open(arg[opt], 'r') as f:
            input_schema = json.load(f)

    return input_schema


if __name__ == '__main__':
    main()
