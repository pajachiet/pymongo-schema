#!/usr/bin/env python
# coding: utf8

"""
pymongo-schema extract schemas from MongoDB 

Usage:
    pymongo-schema  -h | --help
    pymongo-schema  [--database=DB --collection=COLLECTION... --output=FILENAME --format=FORMAT... --port=PORT --host=HOST]

Options:
    
    -d --database DB            Only analyze this database. 
                                By default analyze all datatases in Mongo instance
                                
    -c --collection COL         Only analyze this collection.
                                Multiple collections may be specified this way.
                                
    --port PORT                 Port to connect to [default: 27017]
    
    --host HOST                 Server to connect to [default: localhost]
    
    -o , --output FILENAME      Specify output file name, default to standard output. Extension can be ommited
    
    -f , --format FORMAT        Output format for schema : 'txt', 'yaml' or 'json' [default: txt]
                                Multiple format may be specified.
    
    -h, --help                  show this usage information
"""

from docopt import docopt
import pymongo
from pymongo_schema.export import output_schema
from pymongo_schema.extract import extract_mongo_client_schema
import logging

logger = logging.getLogger('pymongo_schema')
steam_handler = logging.StreamHandler()
steam_handler.setLevel(logging.DEBUG)
logger.addHandler(steam_handler)

if __name__ == '__main__':
    arg = docopt(__doc__, help=True)
    if not arg['--collection']:
        arg['--collection'] = None

    client = pymongo.MongoClient(host=arg['--host'], port=int(arg['--port']))

    schema = extract_mongo_client_schema(client,
                                         database_names=arg['--database'],
                                         collection_names=arg['--collection'])

    for output_format in arg['--format']:
        output_schema(schema, output_format=output_format, filename=arg['--output'])

    if False:
        output_schema(schema, 'yaml', 'schema')
        output_schema(schema, 'txt', 'schema')
        output_schema(schema, 'json', 'schema')
