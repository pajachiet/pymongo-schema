# coding: utf8
from copy import deepcopy
import logging
logger = logging.getLogger(__name__)

PRESENT_VALUE = True


def filter_mongo_schema_namespaces(mongo_schema, namespaces_dict):
    """Filter the schema with namespaces 
    
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
                raise 'TODO implement includeFields', filt
            else:
                raise 'unknown option, not implemented', filt
    return filtered_schema


def init_filtered_schema(namespaces_dict):
    """Initialize filtered_schema dict for databases present in namespace 
    
    :param namespaces_dict: dict
    :return filtered_schema: dict
    """
    filtered_schema = dict()
    for namespace in namespaces_dict.keys():
        db, collection = namespace.split('.', 1)
        filtered_schema[db] = dict()
    return filtered_schema


def exclude_fields_from_collection_schema(exclude_fields_list, collection_schema):
    """Deep copy collection_schema and exclude fields from it.
    
    :param exclude_fields_list: list
    :param collection_schema: dict
    :return collection_schema_copied_filtered: dict 
    """
    collection_schema_copied_filtered = deepcopy(collection_schema)
    exclude_fields_dict = field_list_to_dict(exclude_fields_list)
    exclude_fields_from_object_schema(exclude_fields_dict, collection_schema_copied_filtered)
    return collection_schema_copied_filtered


def exclude_fields_from_object_schema(exclude_fields_dict, object_schema):
    """Exclude fields from object schema, with no return value.
    
    :param exclude_fields_dict: dict
    :param object_schema: dict 
    """
    for field, value in exclude_fields_dict.iteritems():
        if value is PRESENT_VALUE:
            object_schema['object'].pop(field)
        else:
            subfield_dict = value
            field_schema = object_schema['object'][field]
            exclude_fields_from_object_schema(subfield_dict, field_schema)


def field_list_to_dict(field_list):
    """Transform a field list to a recursive field dictionary

    :param field_list: list
    :return field_dict: dict
    """
    fields_dict = dict()
    for field in field_list:
        add_field_to_dict(field, fields_dict)
    return fields_dict


def add_field_to_dict(field, fields_dict):
    """Recursively add a field and its subfield to a field dictionnary. No return value.
    
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
            if fields_dict[parent_field] is not PRESENT_VALUE:  # Parent field is not already forced to be included
                                                                # (priority to upper levels)
                add_field_to_dict(subfield, fields_dict[parent_field])




