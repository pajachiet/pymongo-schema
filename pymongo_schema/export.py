# coding: utf8

import os
import sys
import re
import json
import yaml
import pandas as pd
import logging
logger = logging.getLogger(__name__)


def write_output_dict(output_dict, arg):
    """ Write output dictionary to file or standard output
    
    :param output_dict: dict
        either schema or mapping
    :param output_format: str, 
        either 'json' or 'yaml'
        a special 'txt', 'csv' or 'xlsx' output is possible for mongo schemas
    :param filename: str, default None => standard output
    :param columns_to_get: iterable
        columns to create for each field in 'txt' or 'csv' format
    """
    filename = arg['--output']
    columns_to_get = arg['--columns']

    for output_format in arg['--format']:
        if output_format not in ['txt', 'csv', 'xlsx', 'json', 'yaml']:
            raise ValueError("Ouput format should be txt, csv, json or yaml. {} is not supported".format(output_format))

        # Get output stream
        if filename is None:
            output_file = sys.stdout
            filename = 'standard output'
        else:
            if not filename.endswith('.' + output_format):  # Add extension
                filename += '.' + output_format

            if output_format != 'xlsx':  # Do not open for 'xlsx'
                output_file = open(filename, 'w')

        logger.info('Write output_dict to {} with format {}'.format(filename, output_format))

        # Write output_dict in the correct format
        if output_format in ['csv', 'xlsx']:
            if columns_to_get is None:
                columns_to_get = "Field_full_name Depth Field_name Type".split()

            mongo_schema_df = mongo_schema_as_dataframe(output_dict, columns_to_get)

            if output_format == 'xlsx':
                if filename == 'standard output':
                    print "xlsx format is not supported to standard output. Switching to csv output"
                    output_file = open(filename, 'w')
                    output_format = 'csv'
                else:
                    from openpyxl import load_workbook

                    if os.path.isfile(filename):
                        # Keep existing data
                        # Solution from : http://stackoverflow.com/questions/20219254/how-to-write-to-an-existing-excel-file-without-overwriting-data-using-pandas
                        # May not work for formulaes
                        book = load_workbook(filename)
                        writer = pd.ExcelWriter(filename, engine='openpyxl')
                        writer.book = book
                        writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
                        mongo_schema_df.to_excel(writer, sheet_name='Mongo_Schema', index=False, float_format='{0:.2f}')
                        writer.save()

                    else:
                        mongo_schema_df.to_excel(filename, sheet_name='Mongo_Schema', index=False, float_format='{0:.2f}')

            if output_format == 'csv':
                mongo_schema_df.to_csv(output_file, sep='\t', index=False)

        elif output_format == 'txt':
            if columns_to_get is None:
                columns_to_get = "Field_compact_name Field_name Count Percentage Types_count".split()
            output_str = schema_as_txt(output_dict, columns_to_get)
            output_file.write(output_str + '\n')

        elif output_format == 'json':
            json.dump(output_dict, output_file, indent=4)

        elif output_format == 'yaml':
            yaml.safe_dump(output_dict, output_file, default_flow_style=False)


def mongo_schema_as_dataframe(mongo_schema, columns_to_get):
    """ Represent a MongoDB schema as a dataframe
    
    :param mongo_schema: dict
    :param columns_to_get: iterable
        columns to create for each field
    :return mongo_schema_df: Dataframe
    """
    line_tuples = list()
    for database, database_schema in mongo_schema.iteritems():
        for collection, collection_schema in database_schema.iteritems():
            collection_line_tuples = object_schema_to_line_tuples(collection_schema['object'],
                                                                  columns_to_get,
                                                                  field_prefix='')
            for t in collection_line_tuples:
                line_tuples.append([database, collection] + list(t))

    header = tuple(['Database', 'Collection'] + columns_to_get)
    mongo_schema_df = pd.DataFrame(line_tuples, columns=header)
    return mongo_schema_df


def schema_as_txt(schema, columns_to_get):
    """ Determine mongo schema level and represent it as a string.
      
    Schema level can either be MongoDB instance, Database or Collection
    
    :param schema: dict
    :param columns_to_get: iterable
        columns to create for each field
    :return: str 
    """

    schema_level = get_schema_level(schema)

    if schema_level == 'collection':
        return collection_schema_as_txt(schema, columns_to_get)

    elif schema_level == 'database':
        return database_schema_as_txt(schema, columns_to_get)

    elif schema_level == 'mongo':
        return mongo_schema_as_txt(schema, columns_to_get)


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


def mongo_schema_as_txt(mongo_schema, columns_to_get):
    """ Represent a MongoDB schema as a string

    :param mongo_schema: dict
    :param columns_to_get: iterable
        columns to create for each field
    :param output_format: str, 
        either 'txt' or 'csv'
    :return: str
    """
    database_schema_list = []
    for database, database_schema in mongo_schema.iteritems():
        database_schema_str = database_schema_as_txt(database_schema, columns_to_get)
        database_str = '=' * 20 + '\n' + database + '\n' + database_schema_str
        database_schema_list.append(database_str)

    return '\n\n'.join(database_schema_list)


def database_schema_as_txt(database_schema, columns_to_get):
    """ Represent a Database schema as a string

    :param database_schema: dict
    :param columns_to_get: iterable
        columns to create for each field
    :return: str
    """
    collection_str_list = []
    for collection, collection_schema in database_schema.iteritems():
        count = collection_schema['count']
        collection_schema_str = collection_schema_as_txt(collection_schema, columns_to_get)
        collection_str = '{} {}\n{}'.format(collection, count, collection_schema_str)
        collection_str_list.append(collection_str)

    return '\n\n'.join(collection_str_list)


def collection_schema_as_txt(collection_schema, columns_to_get):
    """ Represent object_schema as readable string

    :param collection_schema: dict
    :param columns_to_get: iterable
        columns to create for each field
    :return object_schema_str: str
    """
    line_tuples = []
    line_tuples += object_schema_to_line_tuples(collection_schema['object'], columns_to_get,
                                                field_prefix='')
    formatting_str = formatting_str_from_tuple_list(line_tuples)
    formatted_lines = []
    for line in line_tuples:
        formatted_lines.append(formatting_str.format(*line))

    object_schema_str = '\n'.join(formatted_lines)
    return object_schema_str


def object_schema_to_line_tuples(object_schema, columns_to_get, field_prefix):
    """ Get the list of tuples describing lines in object_schema

    - Sort fields by count
    - Add the tuples describing each field in object
    - Recursively add tuples for nested objects

    :param object_schema: dict
    :param columns_to_get: iterable
        columns to create for each field
    :param field_prefix: str, default ''
        allows to create full name.
        '.' is the separator for object subfields
        ':' is the separator for list of objects subfields
    :return line_tuples: list of tuples describing lines
    """
    line_tuples = []
    sorted_fields = sorted(object_schema.items(),
                           key=lambda x: x[1]['count'],
                           reverse=True)

    for field, field_schema in sorted_fields:
        line_columns = field_schema_to_columns(field, field_schema, field_prefix, columns_to_get)
        line_tuples.append(line_columns)

        if 'ARRAY' in field_schema['types_count'] and 'OBJECT' in field_schema['array_types_count']:
            line_tuples += object_schema_to_line_tuples(field_schema['object'],
                                                        columns_to_get,
                                                        field_prefix=field_prefix + field + ':')

        elif 'OBJECT' in field_schema['types_count']:  # 'elif' rather than 'if' in case of both OBJECT and ARRAY(OBJECT)
            line_tuples += object_schema_to_line_tuples(field_schema['object'],
                                                        columns_to_get,
                                                        field_prefix=field_prefix + field + '.')

    return line_tuples


def field_schema_to_columns(field, field_schema, field_prefix, columns_to_get):
    """ 
    
    :param field: 
    :param field_schema: 
    :param field_prefix: str, default ''
    :param columns_to_get: iterable
        columns to create for each field
    :return field_columns: tuple
    """
    # f= field
    column_functions = {
        'field_full_name': lambda f, f_schema, f_prefix: f_prefix + f,
        'field_compact_name': field_compact_name,
        'field_name': lambda f, f_schema, f_prefix: f,
        'depth': field_depth,
        'type': field_type,
        'count': lambda f, f_schema, f_prefix: f_schema['count'],
        'proportion_in_object': lambda f, f_schema, f_prefix: f_schema['prop_in_object'],
        'percentage': lambda f, f_schema, f_prefix: 100 * f_schema['prop_in_object'],
        'types_count': lambda f, f_schema, f_prefix:
        format_types_count(f_schema['types_count'], f_schema.get('array_types_count', None)),
    }

    field_columns = list()
    for column in columns_to_get:
        column = column.lower()
        column_str = column_functions[column](field, field_schema, field_prefix)
        field_columns.append(column_str)

    field_columns = tuple(field_columns)
    return field_columns


def field_compact_name(field, field_schema, field_prefix):
    separators = re.sub('[^.:]', '', field_prefix)
    separators = re.sub('.', ' . ', separators)
    separators = re.sub(': ', ' : ', separators)
    return separators + field


def field_depth(field, field_schema, field_prefix):
    separators = re.sub('[^.:]', '', field_prefix)
    return len(separators)


def field_type(field, field_schema, field_prefix):
    f_type = field_schema['type']
    if f_type == 'ARRAY':
        f_type = 'ARRAY(' + field_schema['array_type'] + ')'
    return f_type


def format_types_count(types_count, array_types_count=None):
    """ Format types_count to a readable sting.
    
    >>> format_types_count({'integer': 10, 'boolean': 5, 'null': 3, })
    'integer : 10, boolean : 5, null : 3'
    
    >>> format_types_count({'ARRAY': 10, 'null': 3, }, {'float': 4})
    'ARRAY(float : 4) : 10, null : 3'

    :param types_count: dict
    :param array_types_count: dict, default None 
    :return types_count_string : str
    """
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

    types_count_string = ', '.join(type_count_list)
    return types_count_string


def formatting_str_from_tuple_list(tuple_list, margin=3):
    """ Create the format string from a list of tuple

    The format string is in the form '{0:6}{1:10}', where 
      - '0', '1' indicate the position in format tuple
      - '6', '10' indicate the length of the respective fields

    This function compute for each field the maximum length + margin

    :param tuple_list: list of tuples of str
        all tuples are assumed to have the same length
    :param margin: int, default 3
        margin between successive fields
    :return formatting_str: str
        string to format tuple_list
    """
    formatting_str = ""
    n = len(tuple_list[0])
    for i in range(n):
        field_lengths = [len(str(line[i])) for line in tuple_list]
        field_size = max(field_lengths) + margin
        formatting_str += '{' + str(i) + ':<' + str(field_size) + '}'
    return formatting_str

