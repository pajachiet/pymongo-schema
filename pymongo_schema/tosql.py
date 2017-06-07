# coding: utf8

from mongo_sql_types import psql_type
import logging
logger = logging.getLogger(__name__)

# Type of automatically generated primary keys
# int in default mongo-connector-postgresql, string in pajachiet branch
AUTO_GENERATED_PK_TYPE = 'TEXT'


def mongo_schema_to_mapping(mongo_schema):
    """ Create a mapping to SQL from a mongo schema
    
    :param mongo_schema: dict 
    :return mapping: dict 
    """
    mapping = dict()
    for db in mongo_schema:
        database_schema = mongo_schema[db]
        db_mapping = dict()

        for collection, collection_schema in database_schema.iteritems():
            init_collection_mapping(collection, db_mapping, collection_schema)
            add_object_to_mapping(collection_schema['object'], db_mapping, collection)

        mapping[db] = db_mapping
    return mapping


def init_collection_mapping(collection, mapping, collection_schema):
    """ Initialize a mapping for a collection
    """
    id_mongo_type = collection_schema['object']['_id']['type']
    id_psql_type = psql_type(id_mongo_type)
    mapping[collection] = {
        'pk': '_id',
        '_id': {'type': id_psql_type}
    }
    comment = collection_schema.get('comment', '')
    if comment:
        mapping[collection]['comment'] = comment


def add_object_to_mapping(object_schema, mapping, table_name, field_prefix=''):
    """ Add an object to a mapping 
    
    :param object_schema: dict
    :param mapping: dict
        base mapping dict has to be given, and not only local mapping[collectino], to add 'ARRAY's
    :param table_name: str
    :param field_prefix: str
        used to get full name of imbricated object, from table's parent object (either collection or ARRAY(OBJECT))
    """
    for field, field_info in object_schema.iteritems():
        # Assemble mongo_field_name, the full field name from table's parent object, either collection or ARRAY(OBJECT)
        mongo_field_name = field_prefix + field

        mongo_type = field_info['type']
        if mongo_type == 'ARRAY':
            mongo_array_type = field_info['array_type']
            if mongo_array_type == 'OBJECT':
                add_object_array_to_mapping(mongo_field_name, field_info['object'], mapping, table_name)
            else:
                add_scalar_array_field_to_mapping(field, mongo_field_name, mongo_array_type, mapping, table_name)

        elif mongo_type == 'OBJECT':
            add_object_to_mapping(field_info['object'], mapping, table_name, field_prefix=mongo_field_name + '.')

        else:
            comment = field_info.get('comment', '')
            add_field_to_table_mapping(mongo_field_name, mapping[table_name], mongo_type, comment)


def add_scalar_array_field_to_mapping(field, mongo_field_name, mongo_array_type, mapping, parent_table_name):
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
        logger.warning("WARNING : Mongo type '{}' is not mapped to an SQL type. Scalar array field '{}' from table '{}' is skipped from the mapping."
                       .format(mongo_array_type, mongo_field_name, parent_table_name))
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
    linked_table_name = to_sql_identifier(linked_table_name).replace('.', '__')
    fk_name = 'id_' + parent_table_name

    mapping[parent_table_name][mongo_field_name] = {
        'dest': linked_table_name,
        'fk': fk_name,
    }

    pk_type = get_collection_pk_type(mapping, parent_table_name)
    mapping[linked_table_name] = {
        'pk': 'id',
        fk_name: {
            'type': pk_type
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
        logger.warning("WARNING : Mongo type '{}' is not mapped to an SQL type. Field '{}' from table '{}' is skipped from the mapping."
                       .format(mongo_type, mongo_field_name, table_mapping))
        return

    table_mapping[mongo_field_name] = {
        'dest': to_sql_identifier(mongo_field_name),
        'type': field_psql_type,
    }
    if comment:
        table_mapping[mongo_field_name]['comment'] = comment


def get_collection_pk_type(mapping, table_name):
    """ Get the type of a table primary key in the mapping
    
    :param mapping: dict
    :param table_name: str
    :return collection_pk_type: str
    """
    collection_pk = mapping[table_name]['pk']
    if collection_pk == '_id':
        collection_pk_type = mapping[table_name][collection_pk]['type']
    else:
        collection_pk_type = AUTO_GENERATED_PK_TYPE
    return collection_pk_type


def to_sql_identifier(identifier):
    """ Replace '.' by '__' in identifier, to make it SQL compliant
    """
    return identifier.replace('.', '__')