# coding: utf8

import sys
import json
import yaml
import logging
logger = logging.getLogger(__name__)


def output_schema(schema, output_format='txt', filename=None):
    if output_format not in ['txt', 'json', 'yaml']:
        raise ValueError("Ouput format should be txt, json or yaml. {} is not supported".format(output_format))

    if filename is None:
        output_file = sys.stdout
        filename = 'standard output'
    else:
        if not filename.endswith('.' + output_format):
            filename += '.' + output_format
        output_file = open(filename, 'w')

    logger.info('Write schema to {} with format {}'.format(filename, output_format))

    if output_format == 'txt':
        output_str = schema_as_str(schema)
        output_file.write(output_str + '\n')

    elif output_format == 'json':
        json.dump(schema, output_file, indent=4)

    elif output_format == 'yaml':
        yaml.safe_dump(schema, output_file, default_flow_style=False)


def schema_as_str(schema):
    schema_level = get_schema_level(schema)
    if schema_level == 'collection':
        return collection_schema_as_str(schema)

    elif schema_level == 'database':
        return database_schema_as_str(schema)

    elif schema_level == 'mongo':
        return mongo_schema_as_str(schema)


def get_schema_level(schema):
    """Distinguish between mongo, database or collection schemas level
    
    :param schema: 
    :return: str, 'collection', 'database' or 'mongo'
    """
    if 'object' in schema:
        return 'collection'
    else:
        sub_schema = schema.values()[0]
        if 'object' in sub_schema:  # Collection
            return 'database'
        else:
            return 'mongo'


def mongo_schema_as_str(mongo_schema):
    database_schema_list = []
    for database, database_schema in mongo_schema.iteritems():
        database_schema_str = database_schema_as_str(database_schema)
        database_str = '='*20 + '\n' + database + '\n' + database_schema_str
        database_schema_list.append(database_str)

    return '\n\n'.join(database_schema_list)


def database_schema_as_str(database_schema):
    collection_str_list = []
    for collection, collection_schema in database_schema.iteritems():
        count = collection_schema['count']
        collection_schema_str = collection_schema_as_str(collection_schema)
        collection_str = '{} {}\n{}'.format(collection, count, collection_schema_str)
        collection_str_list.append(collection_str)

    return '\n\n'.join(collection_str_list)


def collection_schema_as_str(collection_schema):
    """Pretty format object_schema as string

    :param object_schema: dict
    :return object_schema_str: str
    """
    lines = [('FIELD_NAME', 'TYPE', 'COUNT', 'PERCENTAGE', 'TYPES_COUNT')]
    lines += object_schema_to_lines_tuples(collection_schema['object'])

    format_str = format_str_for_tuple_list(lines)
    formated_lines = []
    for line in lines:
        formated_lines.append(format_str.format(*line))

    object_schema_str = '\n'.join(formated_lines)
    return object_schema_str


def object_schema_to_lines_tuples(object_schema, field_prefix=''):
    """Get the list of tuples describing lines in object_schema

    - Sort fields by count
    - Add the tuples describing each field in object
    - Recursively add tuples for nested objects

    :param object_schema: dict
    :param field_prefix: str, default ''
    :return lines_tuples: list of tuples describing lines
    """
    lines_tuples = list()

    sorted_fields = sorted(object_schema.items(),
                           key=lambda x: x[1]['count'],
                           reverse=True)

    for field, field_schema in sorted_fields:
        field_name = field_prefix + field
        field_type = field_schema['type']
        if field_type == 'ARRAY':
            field_type = 'ARRAY(' + field_schema['array_type'] + ')'
        field_count = str(field_schema['count'])
        field_percent = str(100 * field_schema['prop_in_object'])
        field_types_count = format_types_count(field_schema['types_count'], field_schema.get('array_types_count', None))
        line = (field_name, field_type, field_count, field_percent, field_types_count)

        lines_tuples.append(line)


        if 'ARRAY' in field_schema['types_count'] and 'OBJECT' in field_schema['array_types_count']:
            lines_tuples += object_schema_to_lines_tuples(field_schema['object'], field_prefix=field_prefix + ' X ')

        elif 'OBJECT' in field_schema['types_count']:  # 'elif' rather than 'if' in case of both OBJECT and ARRAY(OBJECT)
            lines_tuples += object_schema_to_lines_tuples(field_schema['object'], field_prefix=field_prefix + ' . ')

    return lines_tuples


def format_types_count(types_count, array_types_count=None):
    types_count = sorted(types_count.items(),
                         key=lambda x: x[1],
                         reverse=True)

    type_count_list = list()
    for type_name, count in types_count:
        if type_name == 'ARRAY':
            array_type_name = format_types_count(array_types_count)
            type_count_list.append('ARRAY(' + array_type_name + ') : ' + str(count))
        else:
            type_count_list.append(str(type_name) + ' : ' + str(count))
    return ', '.join(type_count_list)



def format_str_for_tuple_list(tuple_list, margin=3):
    """Create the format string for a list of tuple

    The format string is in the form '{0:6}{1:10}', where 
      - '0', '1' indicate the position in format tuple
      - '6', '10' indicate the length of the respective fields

    This function compute for each field the maximum length + margin

    :param tuple_list: list of tuples of str
        all tuples are assumed to have the same length
    :param margin: int, default 3
        margin between successive fields
    :return format_str: str
        string to format tuple_list
    """
    format_str = ""
    n = len(tuple_list[0])
    for i in range(n):
        field_lengths = [len(line[i]) for line in tuple_list]
        field_size = max(field_lengths) + margin
        format_str += '{' + str(i) + ':' + str(field_size) + '}'
    return format_str
