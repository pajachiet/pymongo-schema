#!/usr/bin/env python
# coding: utf8

"""
pymongo-schema extract schemas from MongoDB 

Usage:
    pymongo-schema  -h | --help
    pymongo-schema  [--database=DB --collection=COLLECTION... --output=FILENAME --format=FORMAT... options]

Options:
    
    -d --database DB            Only analyze this database. 
                                By default analyze all datatases in Mongo instance
                                
    -c --collection COL         Only analyze this collection.
                                Multiple collections may be specified this way.
                                
    --port PORT                 Port to connect to [default: 27017]
    
    --host HOST                 Server to connect to [default: localhost]
    
    -o , --output FILENAME      Specify output file name, default to standard output. Extension can be omitted
    
    -f , --format FORMAT        Output format for schema : 'txt', 'yaml' or 'json' [default: txt]
                                Multiple format may be specified.
    
    --quiet                     Desactivate logging to standard output
    
    -h, --help                  show this usage information
"""

from docopt import docopt
import pymongo
from pymongo_schema.export import output_schema
from pymongo_schema.extract import extract_pymongo_client_schema
import logging

if __name__ == '__main__':
    # Parse command line argument
    arg = docopt(__doc__, help=True)
    if not arg['--collection']:
        arg['--collection'] = None

    # Add stream to logger
    if not arg['--quiet']:
        logger = logging.getLogger('pymongo_schema')
        steam_handler = logging.StreamHandler()
        steam_handler.setLevel(logging.DEBUG)
        logger.addHandler(steam_handler)

    # Extract schema
    client = pymongo.MongoClient(host=arg['--host'], port=int(arg['--port']))

    schema = extract_pymongo_client_schema(client,
                                           database_names=arg['--database'],
                                           collection_names=arg['--collection'])
    # Output schema
    for output_format in arg['--format']:
        output_schema(schema, output_format=output_format, filename=arg['--output'])
