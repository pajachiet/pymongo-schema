# coding: utf8

import json
import logging
logger = logging.getLogger(__name__)

PYTHON_TO_PSQL_TYPE = {
    'BOOLEAN': 'BOOLEAN',
    'INTEGER': 'INT',
    'FLOAT': 'REAL',
    'DATE': 'TIMESTAMP',
    'STRING': 'TEXT',
    'OID': 'TEXT',
}

AUTO_GENERATED_IDS_TYPE = 'TEXT'


def mongo_schema_to_mapping(mongo_schema):
    mapping = dict()
    for db in mongo_schema:
        database_schema = mongo_schema[db]
        db_mapping = dict()

        for collection in database_schema.keys():
            init_collection_mapping(collection, db_mapping, database_schema)
            add_object_to_mapping(database_schema[collection]['object'], db_mapping, collection)

        mapping[db] = db_mapping
    return mapping


def init_collection_mapping(collection, mapping, database_schema):
    id_mongo_type = database_schema[collection]['object']['_id']['type']
    id_psql_type = PYTHON_TO_PSQL_TYPE[id_mongo_type]
    mapping[collection] = {
        'pk': '_id',
        '_id': {'type': id_psql_type}
    }


def add_object_to_mapping(object_schema, mapping, collection, field_prefix=''):
    for field, field_info in object_schema.iteritems():

        field_name = field_prefix + field
        field_mongo_type = field_info['type']

        if field_mongo_type == 'DBREF':
            print "field of type DBREF is skipped : " + field_name

        elif field_mongo_type == 'NULL':
            print "field of type NULL is skipped : " + field_name

        elif field_mongo_type == 'ARRAY':
            add_list_field_to_mapping(object_schema, field, field_name, field_info['array_type'], collection, mapping)

        elif field_mongo_type == 'OBJECT':
            add_object_to_mapping(object_schema[field]['object'], mapping, collection, field_prefix=field_name + '.')

        else:
            add_field_to_collection_mapping(field, mapping[collection], field_mongo_type)


def add_list_field_to_mapping(object_schema, field, field_name, list_mongo_type, collection, mapping):
    linked_table_name = initiate_array_mapping(field_name, collection, mapping)

    if list_mongo_type == 'OBJECT':
        mapping[collection][field_name]['type'] = '_ARRAY'
        add_object_to_mapping(object_schema[field]['object'], mapping, linked_table_name)
    else:
        mapping[collection][field_name]['type'] = '_ARRAY_OF_SCALARS'
        mapping[collection][field_name]['valueField'] = field
        add_field_to_collection_mapping(field, mapping[linked_table_name], list_mongo_type)


def initiate_array_mapping(field_name, collection, mapping):
    linked_table_name = to_sql_idenfier(collection + '.' + field_name)
    fk_name = 'id_' + collection
    pk_type = get_collection_pk_type(mapping, collection)
    mapping[collection][field_name] = {
        'dest': linked_table_name,
        'fk': fk_name,
    }

    mapping[linked_table_name] = {
        'pk': 'id',
        fk_name: {
            'type': pk_type
        }
    }
    return linked_table_name


def to_sql_idenfier(mongo_identifier):
    return mongo_identifier.replace('.', '__')


def add_field_to_collection_mapping(field, collection_mapping, field_mongo_type):
    field_psql_type = PYTHON_TO_PSQL_TYPE[field_mongo_type]
    collection_mapping[field] = {'type': field_psql_type}


def get_collection_pk_type(mapping, collection):
    collection_pk = mapping[collection]['pk']
    if collection_pk == '_id':
        collection_pk_type = mapping[collection][collection_pk]['type']
    else:
        collection_pk_type = AUTO_GENERATED_IDS_TYPE
    return collection_pk_type

if __name__ == '__main__':
    main()
