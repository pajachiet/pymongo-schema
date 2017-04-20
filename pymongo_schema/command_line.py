#!/usr/bin/env python
# coding: utf8

"""
pymongo-schema extract schemas from MongoDB 

Usage:
    pymongo-schema  -h | --help
    pymongo-schema  extract [--database=DB --collection=COLLECTION... --output=FILENAME --format=FORMAT... --columns=COLUMNS --port=PORT --host=HOST --quiet]
    pymongo-schema  filter --input=FILENAME --namespace=FILENAME [--output=FILENAME --format=FORMAT... --quiet]
    pymongo-schema  tosql --input=FILENAME [--output=FILENAME --quiet]


Commands:
    extract                     Extract schema from a MongoDB instance
    filter                      Apply a namespace filter to a mongo schema (json input)
    tosql                       Create a mapping from mongo schema to relational schema (json input and output)

Options:
    -d --database DB            Only analyze this database. 
                                By default analyze all databases in Mongo instance
                                
    -c --collection COL         Only analyze this collection.
                                Multiple collections may be specified this way.
                                
    --port PORT                 Port to connect to [default: 27017]
    
    --host HOST                 Server to connect to [default: localhost]
    
    -o , --output FILENAME      Output file for schema. Default to standard output. 
                                Extension added automatically if omitted (useful for multi-format outputs)
    
    -f , --format FORMAT        Output format for schema : 'txt', 'csv', 'yaml' or 'json'
                                Multiple format may be specified. [default: txt]

    --columns HEADER            String listing columns to get in 'txt' or 'csv' format. 
                                Columns are to be chosen in :
                                    FIELD_FULL_NAME         '.' for subfields, ':' for subfields in arrays 
                                    FIELD_COMPACT_NAME      idem, without parent object names
                                    FIELD_NAME              
                                    DEPTH                   
                                    TYPE                    
                                    COUNT                   
                                    PROPORTION_IN_OBJECT    
                                    PERCENTAGE              
                                    TYPES_COUNT             
                                Columns have to be separated by whitespace, and are case insensitive.
                                Default for 'txt' output is "Field_compact_name Field_name Count Percentage Type_count"
                                Default for 'csv' output is "Field_full_name Depth Field_name Type"
                                    
    
    -i , --input FILENAME       Input schema file, to filter or to map to sql. json format expected. 

    -n, --namespace FILENAME    Config file to read namespace to filter. json format expected.
    
    --quiet                     Disable logging to standard output
    
    -h, --help                  show this usage information

"""

import logging
from time import time
from docopt import docopt
import json
import pymongo
from export import write_output_dict
from extract import extract_pymongo_client_schema
from filter import filter_mongo_schema_namespaces
from tosql import mongo_schema_to_mapping

logger = logging.getLogger()


def main():
    """ Launch pymongo_schema (assuming CLI)
    """
    # Parse command line argument
    arg = docopt(__doc__, help=True)
    if not arg['--collection']:
        arg['--collection'] = None

    initialize_logger(arg)

    # Extract mongo schema
    if arg['extract']:
        output_dict = extract_schema(arg)

    # Filter mongo schema by namespace
    if arg['filter']:
        output_dict = filter_schema(arg)

    # Map mongo schema to sql
    if arg['tosql']:
        arg['--format'] = ['json']
        output_dict = schema_to_sql(arg)

    # Output dict
    logger.info('=== Write MongoDB schema')
    for output_format in arg['--format']:
        write_output_dict(output_dict, output_format=output_format, filename=arg['--output'], columns_to_get=arg['--columns'])


def initialize_columns(arg):
    if arg['--columns'] is None:
        arg['--columns'] = "FIELD_COMPACT_NAME FIELD_NAME TYPE COUNT PERCENTAGE TYPES_COUNT"

    arg['--columns'] = arg['--columns'].split()


def initialize_logger(arg):
    """ Initialize logging to standard output, if not quiet.  
    """
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.NullHandler())

    if not arg['--quiet']:
        steam_handler = logging.StreamHandler()
        logger.addHandler(steam_handler)


def extract_schema(arg):
    """ Main entry point function to extract schema  
    
    :param arg: 
    :return mongo_schema: dict 
    """
    start_time = time()
    logger.info('=== Start MongoDB schema analysis')
    client = pymongo.MongoClient(host=arg['--host'], port=int(arg['--port']))

    mongo_schema = extract_pymongo_client_schema(client,
                                                 database_names=arg['--database'],
                                                 collection_names=arg['--collection'])

    logger.info('--- MongoDB schema analysis took {:.2f} s'.format(time() - start_time))
    return mongo_schema


def filter_schema(arg):
    """ Main entry point function to filter schema by a namespace
    
    :param arg: dict
    :return filtered_mongo_schema: dict
    """
    logger.info('=== Filter mongo schema')
    with open(arg['--namespace'], 'r') as f:
        config = json.load(f)

    with open(arg['--input'], 'r') as f:
        input_schema = json.load(f)

    filtered_mongo_schema = filter_mongo_schema_namespaces(input_schema, config['namespaces'])
    return filtered_mongo_schema


def schema_to_sql(arg):
    """ Main entry point function to genearate a mapping from mongo to sql
    
    :param arg: dict
    :return mongo_to_sql_mapping: dict 
    """
    logger.info('=== Generate mapping from mongo to sql')
    with open(arg['--input']) as data_file:
        mongo_schema = json.load(data_file)

    mongo_to_sql_mapping = mongo_schema_to_mapping(mongo_schema)
    return mongo_to_sql_mapping


if __name__ == '__main__':
    main()