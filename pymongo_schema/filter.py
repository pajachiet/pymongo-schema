# coding: utf8
from copy import deepcopy
import logging
logger = logging.getLogger(__name__)

PRESENT_VALUE = 'present'

def filter_mongo_schema_namespaces(mongo_schema, namespaces_dict):
    """ Filter the schema with namespaces 
    
    :param mongo_schema: dict
    :param namespaces_dict: dict
    :return filtered_schema: dict
    """
    filtered_schema = init_filtered_schema(namespaces_dict)

    for namespace, filt in namespaces_dict.iteritems():
        if filt is False:
            continue

        db, collection = namespace.split('.', 1)

        if db not in mongo_schema:
            logger.warning('WARNING : Database {} is supposed to be filtered, but is not present in mongo schema'.format(db))
            continue
        database_schema = mongo_schema[db]

        if collection not in database_schema:
            logger.warning('WARNING : Collection {} is supposed to be filtered from database {}, but is not present in mongo schema'.format(collection, db))
            continue
        collection_schema = database_schema[collection]

        if filt is True:
            filtered_schema[db][collection] = collection_schema
        else:
            if 'excludeFields' in filt:
                logger.info("Exclude fields from collection " + collection)
                exclude_fields = filt['excludeFields']
                filtered_schema[db][collection] = exclude_fields_from_collection_schema(exclude_fields, collection_schema)
            elif 'includeFields' in filt:
                logger.info("Include fields from collection " + collection)
                include_fields = filt['includeFields']
                filtered_schema[db][collection] = include_fields_from_collection_schema(include_fields, collection_schema)
            else:
                raise NotImplementedError('unknown option, not implemented : {}'.format(filt.keys()))
    return filtered_schema


def init_filtered_schema(namespaces_dict):
    """ Initialize filtered_schema dict for databases present in namespace 
    
    :param namespaces_dict: dict
    :return filtered_schema: dict
    """
    filtered_schema = dict()
    for namespace in namespaces_dict.keys():
        db, collection = namespace.split('.', 1)
        filtered_schema[db] = dict()
    return filtered_schema


def include_fields_from_collection_schema(include_fields_list, collection_schema):
    """ Copy collection_schema, keeping only included fields 
    >>> collection_schema = {\
        'object': {\
            'field':'field_schema',\
            'other_field':'field_schema',\
            'sub': {\
                'object': {\
                    'field':'field_schema',\
                    'other_field':'field_schema',\
                    }\
            }\
        }\
    }
    >>> include_fields_from_collection_schema(['field', 'sub.field'], collection_schema)
    {'object': {'field': 'field_schema', 'sub': {'object': {'field': 'field_schema'}}}}

    :param include_fields_list: list
    :param collection_schema: dict
    """
    include_fields_dict = field_list_to_dict(include_fields_list)
    return include_fields_from_object_schema(include_fields_dict, collection_schema)


def include_fields_from_object_schema(include_fields_dict, object_count_schema):
    """ Add fields from include_fields_dict to object schema, with no return value.

    :param include_fields_dict: dict
    :param object_count_schema: dict 
    """
    object_schema_filtered = {k: v for k, v in object_count_schema.iteritems() if k != 'object'}
    object_schema_filtered['object'] = {}

    object_schema = object_count_schema['object']
    for field, value in include_fields_dict.iteritems():
        if field not in object_schema:
            logger.warn("WARNING: Field '{}' is not in schema but is present in includeFields".format(field))
            continue

        if value is PRESENT_VALUE:
            object_schema_filtered['object'][field] = object_schema[field]
        else:
            subfield_dict = value
            field_schema = object_schema[field]
            object_schema_filtered['object'][field] = include_fields_from_object_schema(subfield_dict, field_schema)

    return object_schema_filtered

def exclude_fields_from_collection_schema(exclude_fields_list, collection_schema):
    """ Deep copy collection_schema and exclude fields from it.
    
    >>> collection_schema = {\
        'object': {\
            'field':'field_schema',\
            'other_field':'field_schema',\
            'sub': {\
                'object': {\
                    'field':'field_schema',\
                    'other_field':'field_schema',\
                    }\
            }\
        }\
    }
    >>> exclude_fields_from_collection_schema(['field', 'sub.field'], collection_schema)
    {'object': {'other_field': 'field_schema', 'sub': {'object': {'other_field': 'field_schema'}}}}

    :param exclude_fields_list: list
    :param collection_schema: dict
    :return collection_schema_copied_filtered: dict 
    """
    collection_schema_copied_filtered = deepcopy(collection_schema)
    exclude_fields_dict = field_list_to_dict(exclude_fields_list)
    exclude_fields_from_object_schema(exclude_fields_dict, collection_schema_copied_filtered['object'])
    return collection_schema_copied_filtered


def exclude_fields_from_object_schema(exclude_fields_dict, object_schema):
    """ Exclude fields from object schema, with no return value.
    
    :param exclude_fields_dict: dict
    :param object_schema: dict 
    """
    for field, value in exclude_fields_dict.iteritems():
        if value is PRESENT_VALUE:
            if field not in object_schema:
                logger.warn("WARNING: Field '{}' is not in schema but is present in excludeFields".format(field))
                continue
            object_schema.pop(field)
        else:
            subfield_dict = value
            field_schema = object_schema[field]
            exclude_fields_from_object_schema(subfield_dict, field_schema['object'])


def field_list_to_dict(field_list):
    """ Transform a field list to a recursive field dictionary
    
    >>> field_list_to_dict(['field', 'sub.field'])
    {'field': 'present', 'sub': {'field': 'present'}}
    
    :param field_list: list
    :return field_dict: dict
    """
    fields_dict = dict()
    for field in field_list:
        add_field_to_dict(field, fields_dict)
    return fields_dict


def add_field_to_dict(field, fields_dict):
    """ Recursively add a field and its subfield to a field dictionnary. No return value.
        
    :param field: str
    :param fields_dict: dict 
    """
    if '.' not in field:
        fields_dict[field] = PRESENT_VALUE
    else:
        parent_field, subfield = field.split('.', 1)
        if parent_field not in fields_dict:
            fields_dict[parent_field] = dict()
            add_field_to_dict(subfield, fields_dict[parent_field])
        else:
            if not fields_dict[parent_field] is PRESENT_VALUE:  # Parent field is not already forced to be included
                                                                 # (priority to upper levels)
                add_field_to_dict(subfield, fields_dict[parent_field])



