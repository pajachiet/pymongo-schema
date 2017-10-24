# coding: utf8
"""
This module intends to translate a MongoDB schema (from extract) into a 'mapping'.

A mapping is meant to map a mongo schema to a postgresql one, as defined by:
https://github.com/Hopwork/mongo-connector-postgresql
"""


import logging

from pymongo_schema.mongo_sql_types import psql_type

logger = logging.getLogger(__name__)

# Type of automatically generated primary keys
# It may depend from mongo-connector-postgresql branch in use
AUTO_GENERATED_PK_TYPE = 'SERIAL'


def mongo_schema_to_mapping(mongo_schema):
    """ Create a mapping to SQL from a mongo schema

    :param mongo_schema: dict
    :return mapping: dict
    """
    mapping = dict()
    for db in mongo_schema:
        database_schema = mongo_schema[db]
        db_mapping = dict()

        for collection, collection_schema in database_schema.items():
            if not collection_schema['object']:
                logger.warning("Collection '{}' is skipped has its schema is empty.")
                continue
            if not init_collection_mapping(collection, db_mapping, collection_schema):
                logger.warning("Collection '{}' is skipped because initialisation of its mapping failed.")
                continue
            add_object_to_mapping(collection_schema['object'], db_mapping, collection)

        mapping[db] = db_mapping
    return mapping


def init_collection_mapping(collection, mapping, collection_schema):
    """ Initialize a mapping for a collection
    """
    id_mongo_type = collection_schema['object']['_id']['type']

    try:
        id_psql_type = psql_type(id_mongo_type)
    except KeyError:
        logger.warning("WARNING : Mongo type '%s' is not mapped to an SQL type. As this field is "
                       "an '_id', the entire collection is skipped from the mapping.",
                       id_mongo_type)
        return False

    mapping[collection] = {
        'pk': '_id',
        '_id': {'type': id_psql_type}
    }
    comment = collection_schema.get('comment', '')
    if comment:
        mapping[collection]['comment'] = comment

    return True


def add_object_to_mapping(object_schema, mapping, table_name, field_prefix=''):
    """ Add an object to a mapping

    :param object_schema: dict
    :param mapping: dict
        base mapping dict has to be given, and not only local mapping[collection], to add 'ARRAY's
    :param table_name: str
    :param field_prefix: str
        used to get full name of nested object,
        from table's parent object (either collection or ARRAY(OBJECT))
    """
    for field, field_info in object_schema.items():
        # Assemble mongo_field_name, the full field name from table's parent object,
        # either collection or ARRAY(OBJECT)
        mongo_field_name = field_prefix + field

        mongo_type = field_info['type']
        if mongo_type == 'ARRAY':
            mongo_array_type = field_info['array_type']
            if mongo_array_type == 'OBJECT':
                add_object_array_to_mapping(mongo_field_name, field_info['object'],
                                            mapping, table_name)
            else:
                add_scalar_array_field_to_mapping(field, mongo_field_name, mongo_array_type,
                                                  mapping, table_name)

        elif mongo_type == 'OBJECT':
            if 'object' in field_info:
                add_object_to_mapping(field_info['object'], mapping, table_name,
                                      field_prefix=mongo_field_name + '.')
            else:   # can happen in extract from code (DictField)
                logger.warning(
                    "WARNING : 'JSON' SQL type is not managed yet. Field '%s' from table "
                    "'%s' is skipped from the mapping.",
                    mongo_field_name, table_name)

        else:
            comment = field_info.get('comment', '')
            add_field_to_table_mapping(mongo_field_name, mapping[table_name], mongo_type, comment)


def add_scalar_array_field_to_mapping(field, mongo_field_name, mongo_array_type, mapping,
                                      parent_table_name):
    """ Add a linked table to the mapping, corresponding to an array of scalars

    :param field: str
        local field name, used to define column name (valueField) in target mapping
    :param mongo_field_name: str
        full field name from table's parent object, either collection or ARRAY(OBJECT)
    :param mongo_array_type: str
    :param mapping: dict
    :param parent_table_name: str
    """
    try:
        psql_type(mongo_array_type)
    except KeyError:
        logger.warning("WARNING : Mongo type '%s' is not mapped to an SQL type. Scalar array "
                       "field '%s' from table '%s' is skipped from the mapping.",
                       mongo_array_type, mongo_field_name, parent_table_name)
        return

    linked_table_name = initiate_array_mapping(mongo_field_name, mapping, parent_table_name)
    mapping[parent_table_name][mongo_field_name]['type'] = '_ARRAY_OF_SCALARS'
    mapping[parent_table_name][mongo_field_name]['valueField'] = field
    add_field_to_table_mapping(field, mapping[linked_table_name], mongo_array_type)


def add_object_array_to_mapping(mongo_field_name, object_schema, mapping, parent_table_name):
    """ Add a linked table to the mapping, corresponding to an array of objects

    :param mongo_field_name: str
        full field name from table's parent object, either collection or ARRAY(OBJECT)
    :param object_schema: str
    :param mapping: dict
    :param parent_table_name: str
    """
    linked_table_name = initiate_array_mapping(mongo_field_name, mapping, parent_table_name)
    mapping[parent_table_name][mongo_field_name]['type'] = '_ARRAY'
    add_object_to_mapping(object_schema, mapping, linked_table_name, field_prefix='')


def initiate_array_mapping(mongo_field_name, mapping, parent_table_name):
    """ Initiate an array mapping.

    Common part between either object or scalar arrays.

    - initialize field in original collection
    - initialize linked table

    :param mongo_field_name: str
        full field name from table's parent object, either collection or ARRAY(OBJECT)
    :param parent_table_name: str
    :param mapping:
    :return linked_table_name: str
    """
    linked_table_name = parent_table_name + '.' + mongo_field_name
    linked_table_name = to_sql_identifier(linked_table_name)
    fk_name = 'id_' + parent_table_name

    mapping[parent_table_name][mongo_field_name] = {
        'dest': linked_table_name,
        'fk': fk_name,
    }

    # Get type of parent table primary key
    parent_table_pk = mapping[parent_table_name]['pk']
    if parent_table_pk == '_id':
        parent_table_pk_type = mapping[parent_table_name][parent_table_pk]['type']
    else:
        parent_table_pk_type = AUTO_GENERATED_PK_TYPE

    mapping[linked_table_name] = {
        'pk': '_id_postgres',
        fk_name: {
            'type': parent_table_pk_type
        }
    }
    return linked_table_name


def add_field_to_table_mapping(mongo_field_name, table_mapping, mongo_type, comment=""):
    """ Add a field to a table mapping

    :param mongo_field_name: str
        full field name from table's parent object, either collection or ARRAY(OBJECT)
    :param table_mapping: dict
    :param mongo_type: str
    """
    try:
        field_psql_type = psql_type(mongo_type)
    except KeyError:
        logger.warning("WARNING : Mongo type '%s' is not mapped to an SQL type. Field '%s' is "
                       "skipped from the mapping.", mongo_type, mongo_field_name)
        return

    table_mapping[mongo_field_name] = {
        'dest': to_sql_identifier(mongo_field_name),
        'type': field_psql_type,
    }
    if comment:
        table_mapping[mongo_field_name]['comment'] = comment


def to_sql_identifier(identifier):
    """ Replace character in MongoDB identifier that are illegal in SQL
    """
    return identifier.replace('.', '__').replace('-', '_').replace(' ', '_')
