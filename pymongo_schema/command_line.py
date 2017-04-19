#!/usr/bin/env python
# coding: utf8

"""
pymongo-schema extract schemas from MongoDB 

Usage:
    pymongo-schema  -h | --help
    pymongo-schema  extract [--database=DB --collection=COLLECTION... --output=FILENAME --format=FORMAT... --port=PORT --host=HOST --quiet]
    pymongo-schema  filter --input=FILENAME --namespace=FILENAME [--output=FILENAME --format=FORMAT... --quiet]
    pymongo-schema  tosql --input=FILENAME [--output=FILENAME --quiet]


Commands:
    extract                     Extract schema from a MongoDB instance
    filter                      Apply a namespace filter to a mongo schema
    tosql                       Create a mapping from mongo schema to relational schema

Options:
    -d --database DB            Only analyze this database. 
                                By default analyze all databases in Mongo instance
                                
    -c --collection COL         Only analyze this collection.
                                Multiple collections may be specified this way.
                                
    --port PORT                 Port to connect to [default: 27017]
    
    --host HOST                 Server to connect to [default: localhost]
    
    -o , --output FILENAME      Output file for schema. Default to standard output. 
                                Extension added automatically if omitted (useful for multi-format outputs)
    
    -f , --format FORMAT        Output format for schema : 'txt', 'yaml' or 'json'
                                Multiple format may be specified. [default: txt]
    
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
from export import output_schema
from extract import extract_pymongo_client_schema
from filter import filter_mongo_schema_namespaces
from tosql import mongo_schema_to_mapping

logger = logging.getLogger()


def inititialize_logger(arg):
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.NullHandler())

    if not arg['--quiet']:
        steam_handler = logging.StreamHandler()
        logger.addHandler(steam_handler)

    return logger


def extract_schema(arg):
    start_time = time()
    logger.info('=== Start MongoDB schema analysis')
    client = pymongo.MongoClient(host=arg['--host'], port=int(arg['--port']))

    schema = extract_pymongo_client_schema(client,
                                           database_names=arg['--database'],
                                           collection_names=arg['--collection'])

    logger.info('--- MongoDB schema analysis took {:.2f} s'.format(time() - start_time))
    return schema


def filter_schema(arg):
    logger.info('=== Filter mongo schema')
    with open(arg['--namespace'], 'r') as f:
        config = json.load(f)

    with open(arg['--input'], 'r') as f:
        input_schema = json.load(f)
    return filter_mongo_schema_namespaces(input_schema, config['namespaces'])


def schema_to_sql(arg):
    logger.info('=== Generate mapping from mongo to sql')
    with open(arg['--input']) as data_file:
        mongo_schema = json.load(data_file)

    return mongo_schema_to_mapping(mongo_schema)


def main():
    """ Launch pymongo_schema (assuming CLI)
    """
    # Parse command line argument
    arg = docopt(__doc__, help=True)
    if not arg['--collection']:
        arg['--collection'] = None

    logger = inititialize_logger(arg)

    # Extract schema
    if arg['extract']:
        output_dict = extract_schema(arg)

    # Filter schema
    if arg['filter']:
        output_dict = filter_schema(arg)

    if arg['tosql']:
        arg['--format'] = ['json']
        output_dict = schema_to_sql(arg)

    # Output schema
    logger.info('=== Write MongoDB schema')
    for output_format in arg['--format']:
        output_schema(output_dict, output_format=output_format, filename=arg['--output'])


if __name__ == '__main__':
    main()