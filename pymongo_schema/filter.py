# coding: utf8
"""
This module intends to filter a mongo schema based on given namespace dictionary.

The mongo schema comes from the 'extract' module.
The namespace uses mongo-connector config format:

    {db_name_1.coll_name_1 : True,
     db_name_1.coll_name_2 : False,
     db_name_1.coll_name_1 : {"includeFields: [field1, field2.subfield1]},
     db_name_1.coll_name_1 : {"excludeFields: [field1, field2.subfield1]}}
"""
from copy import deepcopy
import logging

logger = logging.getLogger(__name__)

# Schema of fields to include is based on keys of a dict.
# PRESENT_VALUE is used as the value for those keys to keep
PRESENT_VALUE = 'present'


def filter_mongo_schema_namespaces(mongo_schema, namespaces_dict):
    """ Filter the schema with namespaces

    :param mongo_schema: dict
    :param namespaces_dict: dict
    :return filtered_schema: dict
    """
    filtered_schema = init_filtered_schema(namespaces_dict)

    for namespace, filt in namespaces_dict.items():
        if filt is False:
            continue

        db, collection = namespace.split('.', 1)

        if db not in mongo_schema:
            logger.warning('WARNING : Database %s is supposed to be filtered, but is not present '
                           'in mongo schema', db)
            continue
        database_schema = mongo_schema[db]

        if collection == '*':
            collection_list = database_schema.keys()
        else:
            collection_list = [collection]

        for collection in collection_list:
            if collection not in database_schema:
                logger.warning('WARNING : Collection %s is supposed to be filtered from database '
                               '%s, but is not present in mongo schema', collection, db)
                continue
            collection_schema = database_schema[collection]

            if filt is True:
                logger.info("Include the whole collection %s", collection)
                filtered_schema[db][collection] = collection_schema
            else:
                if 'excludeFields' in filt:
                    logger.info("Exclude fields from collection %s", collection)
                    filtered_schema[db][collection] = exclude_fields_from_collection_schema(
                        filt['excludeFields'], collection_schema)
                elif 'includeFields' in filt:
                    logger.info("Include fields from collection %s", collection)
                    filtered_schema[db][collection] = include_fields_from_collection_schema(
                        filt['includeFields'], collection_schema)
                else:
                    raise NotImplementedError('unknown option, not implemented : %s', filt.keys())

    for db in list(filtered_schema):
        if not filtered_schema[db]:
            del filtered_schema[db]
    return filtered_schema


def init_filtered_schema(namespaces_dict):
    """ Initialize filtered_schema dict for databases present in namespace

    :param namespaces_dict: dict
    :return filtered_schema: dict
    """
    filtered_schema = dict()
    for namespace in namespaces_dict:
        db = namespace.split('.')[0]
        filtered_schema[db] = dict()
    return filtered_schema


def include_fields_from_collection_schema(include_fields_list, collection_schema):
    """ Copy collection_schema, keeping only included fields.
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
    """ Add fields from include_fields_dict to object schema.

    :param include_fields_dict: dict
    :param object_count_schema: dict
    """
    object_schema_filtered = {k: v for k, v in object_count_schema.items() if k != 'object'}
    object_schema_filtered['object'] = {}

    object_schema = object_count_schema['object']
    for field, value in include_fields_dict.items():
        if field not in object_schema:
            logger.warning("WARNING: Field '%s' is present in includeFields but not in schema",
                           field)
            continue

        if value is PRESENT_VALUE:
            object_schema_filtered['object'][field] = object_schema[field]
        else:
            subfield_dict = value
            field_schema = object_schema[field]
            object_schema_filtered['object'][field] = include_fields_from_object_schema(
                subfield_dict, field_schema)

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
    exclude_fields_from_object_schema(exclude_fields_dict,
                                      collection_schema_copied_filtered['object'])
    return collection_schema_copied_filtered


def exclude_fields_from_object_schema(exclude_fields_dict, object_schema):
    """ Exclude fields from object schema, with no return value.

    :param exclude_fields_dict: dict
    :param object_schema: dict
    """
    for field, value in exclude_fields_dict.items():
        if field not in object_schema:
            logger.warning("WARNING: Field '%s' is present in excludeFields, but not in schema",
                           field)
            continue

        if value is PRESENT_VALUE:
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
    """ Recursively add a field and its subfield to a field dictionary. No return value.

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
            if not fields_dict[parent_field] is PRESENT_VALUE:
                # Test if parent field is not already forced to be included
                add_field_to_dict(subfield, fields_dict[parent_field])
