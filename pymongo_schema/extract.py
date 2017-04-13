# coding: utf8
"""
We define 'schema' as a dictionnary describing the structure of MongoDB component. 
We have schemas for MongoDB instances, databases, collections, objects and fields. 
Functions in this library which take a 'schema' as argument, modify this schema as a side-effect and have no value.

Schema are hierarchically nested :  
- A MongoDB instance contains databases
    {
        "database_name_1": database_schema_1,
        "database_name_2": database_schema_2            
    }
- A database contains collections
    {
        "collection_name_1": collection_schema_1,
        "collection_name_2": collection_schema_2
    }

- A collection maintains a 'count' and contains 1 object
    { 
        "count" : int, 
        "object": object_schema 
    }

- An object contains fields.
Objects are initialized as defaultdict(empty_field_schema) to simplify the code 
    { 
        "field_name_1" : field_schema_1, 
        "field_name_2": field_schema_2 
    }

- A field maintains 'type', 'count' and 'null_count' information 
An optional 'ARRAY' field maintains an 'array_type' if the field is an ARRAY
An 'OBJECT' or 'ARRAY(OBJECT)' field recursively contains 1 'object'
    {
        'type': type_name,
        'count': int,
        'null_count': int, # DELETED while postprocessing if 0 
        'list_type': 'NULL' # OPTIONAL : if the field is an ARRAY
        'object': object_schema # OPTIONAL : if the field is a nested document
    }    
"""

import bson
from collections import defaultdict

TYPE_TO_STR = {
    bson.datetime.datetime: "DATE",
    bson.timestamp.Timestamp: "TIMESTAMP",
    int: "INTEGER",
    bson.int64.Int64: "INTEGER",
    float: "FLOAT",
    unicode: "STRING",
    bson.objectid.ObjectId: "OID",
    list: "ARRAY",
    dict: "OBJECT",
    #    type(None): "NULL",
    bson.dbref.DBRef: "DBREF",
    bool: "BOOLEAN"
}


def extract_mongo_client_schema(pymongo_client, database_names=None):
    """Extract the schema for every database in database_names
    
    :param pymongo_client: pymongo.mongo_client.MongoClient
    :param database_names: list of str, default None
    :return mongo_schema: dict
    """

    if database_names is None:
        database_names = pymongo_client.database_names()
        database_names.remove('admin')
        database_names.remove('local')


    mongo_schema = dict()
    for database in database_names:
        pymongo_database = pymongo_client[database]
        mongo_schema[database] = extract_database_schema(pymongo_database)

    return mongo_schema


def extract_database_schema(pymongo_database, collection_names=None):
    """Extract the database schema, for every collection in collection_names

    :param pymongo_database: pymongo.database.Database
    :param collection_names: list of str, default None
    :return database_schema: dict
    """
    if collection_names is None:
        collection_names = pymongo_database.collection_names()

    database_schema = dict()
    for collection in collection_names:
        pymongo_collection = pymongo_database[collection]
        database_schema[collection] = extract_collection_schema(pymongo_collection)

    return database_schema


def extract_collection_schema(pymongo_collection):
    """Iterate through all document of a collection to create its schema

    - Init collection schema
    - Add every document from MongoDB collection to the schema
    - Post-process schema

    :param pymongo_collection: pymongo.collection.Collection
    :return collection_schema: dict
    """
    collection_schema = {
        'count': 0,
        "object": init_empty_object_schema()
    }

    for document in pymongo_collection.find({}):
        collection_schema['count'] += 1
        add_document_to_object_schema(document, collection_schema['object'])

    post_process_schema(collection_schema)
    collection_schema = recursive_default_to_regular_dict(collection_schema)
    return collection_schema


def recursive_default_to_regular_dict(value):
    """If value is a dictionnary, recursively replace defaultdict to regular dict 
    
    Note : defaultdict are instances of dict
    
    :param value: 
    :return d: dict or original value
    """
    if isinstance(value, dict):
        d = {k: recursive_default_to_regular_dict(v) for k, v in value.iteritems()}
        return d
    else:
        return value


def post_process_schema(object_count_schema):
    """Clean and add information to schema once it has been built

    - delete 'null_count' if 0
    - compute the proportion of non null values in the parent object
    - recursively postprocess imbricated object schemas

    :param object_count_schema: dict
    This schema can either be a field_schema or a collection_schema
    """
    object_count = object_count_schema['count']
    object_schema = object_count_schema['object']
    for field_schema in object_schema.values():
        if field_schema['null_count'] == 0:
            del field_schema['null_count']

        field_schema['prop_in_object'] = round((field_schema['count']) / float(object_count), 5)

        if 'object' in field_schema:
            post_process_schema(field_schema)


def init_empty_object_schema():
    """Generate an empty object schema.

    We use a defaultdict of empty fields schema. This avoid to test for the presence of fields.
    :return: defaultdict(empty_field_schema)
    """

    def empty_field_schema():
        field_dict = {
            'type': "NULL",
            'count': 0,
            'null_count': 0,
        }
        return field_dict

    empty_object = defaultdict(empty_field_schema)
    return empty_object


def add_document_to_object_schema(document, object_schema):
    """Add a all fields of a document to a local object_schema.

    :param document: dict
    contains a MongoDB Object
    :param object_schema: dict
    """
    for field, value in document.iteritems():
        add_value_to_field_schema(value, object_schema[field])


def add_value_to_field_schema(value, field_schema):
    """Add a value to a field_schema

    - Update count or 'null_count' count.
    - Define or check the type of value.
    - Recursively add 'list' and 'dict' value to the schema.

    :param value:
    value corresponding to a field in a MongoDB Object
    :param field_schema: dict
    subdictionnary of the global schema dict corresponding to a field
    """
    if value is None or value == []:
        field_schema['null_count'] += 1
    else:
        field_schema['count'] += 1
        define_or_check_value_type(value, field_schema)
        add_potential_list_to_field_schema(value, field_schema)
        add_potential_document_to_field_schema(value, field_schema)


def add_potential_document_to_field_schema(document, field_schema):
    """Add a document to a field_schema
    
    - Exit if document is not a dict
    
    :param document: dict (or skipped)
    :param field_schema: 
    """
    if isinstance(document, dict):
        if 'object' not in field_schema:
            field_schema['object'] = init_empty_object_schema()
        add_document_to_object_schema(document, field_schema['object'])


def add_potential_list_to_field_schema(value_list, field_schema):
    """Add a list of values to a field_schema

    - Exit if value_list is not a list
    - Define or check the type of each value of the list.
    - Recursively add 'dict' values to the schema.   

    :param value_list: list (or skipped) 
    :param field_schema: dict
    """
    if isinstance(value_list, list):
        if 'list_type' not in field_schema:
            field_schema['list_type'] = 'NULL'

        for value in value_list:
            define_or_check_value_type(value, field_schema, type_str='list_type')
            add_potential_document_to_field_schema(value, field_schema)


def define_or_check_value_type(value, field_schema, type_str='type'):
    """Define the type_str in field_schema, or check it is equal to the one previously defined. 

    :param value: 
    :param field_schema: dict
    :param type_str: str
    """
    value_type_str = TYPE_TO_STR[type(value)]
    if field_schema[type_str] == "NULL":
        field_schema[type_str] = value_type_str
    else:
        assert field_schema[type_str] == value_type_str  # TODO: raise an error and catch it above
