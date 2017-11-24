# coding: utf8
"""
This module intends to transform a json (schema, mapping, ...) and write it in different file types.

Entry point is write_output_dict.

It relies on a class system that defines how to open and write into the different file types.

The base class is BaseOutput, it defines the mechanism.
abstract property : output_format
abstract method: write_data
Those must be overridden in the final subclasses.
The opener method should be overridden as well if open method is not enough
(to manage non ascii for example).

It is inherited by two base classes, that represent two groups of outputs:
HierarchicalOutput for nested formats (yaml and json)
ListOutput for table like formats (tsv, md, html - since this format displays a table, xlsx).
They intend to preprocess data as this is common to each group of output.
They use OutputPreProcessing class to deal with this preprocessing.
This class is a factory that will allow to use the right preprocessing methods
depending on the category treated (schema, mapping, ...).

Then those base classes are used (inherited from) to define each format:
JsonOutput, YamlOutput, TsvOutput, HtmlOutput, MdOutput, XlsxOutput
"""
import abc
import codecs
import csv
import json
import logging
import os
import re
import sys
from contextlib import contextmanager
from functools import partial
from numbers import Number

import yaml
import jinja2
from bson import json_util
from future.moves.collections import OrderedDict
from past.builtins import basestring
import pandas as pd
from openpyxl import load_workbook

logger = logging.getLogger(__name__)


class BaseOutput(object):
    """
    Abstract base class. Defines the mechanism allowing to write data into various file formats.

    The principle is to be able to define how to open the file and how to write into it
    using the same interface no matter what format is asked.

    Abstract methods to override:
    property output_format (can be class attribute): string representing the format expected
        ('json', 'yaml', 'tsv', 'html', 'md, 'xlsx', ... it should match file extension)
    write_data: define how to write into the object given by the open method

    Public methods that can be overridden to specialize the output:
    opener
    closer

    Other public method (should not be overridden):
    open: context manager that yields the file ready to be written into by write_data.
        It uses private methods opener and closer to define how to open and close the file
    """
    __metaclass__ = abc.ABCMeta

    @property
    @abc.abstractmethod
    def output_format(self):
        """Expected string matching the extension of the file format."""
        pass

    @contextmanager
    def open(self, filename):
        """Yields a file descriptor as expected in self.write_data"""
        if not filename:  # output is stdout, opener and closer must be adapted
            self.opener = self._stdout_opener
            self.closer = self._stdout_closer
        else:
            if not filename.endswith('.' + self.output_format):  # Add extension
                filename += '.' + self.output_format
        file_descr = self.opener()(filename)
        try:
            yield file_descr
        finally:
            self.closer(file_descr)

    def opener(self):
        """Return the function used to open the file (only filename will be passed as argument)."""
        return partial(open, mode='w')

    def _stdout_opener(self):
        """Mock opener if output is stdout"""
        return lambda x: sys.stdout

    def closer(self, file_descr):
        """Actions to perform to close the file."""
        try:
            file_descr.close()
        except AttributeError:
            pass

    def _stdout_closer(self, file_descr):
        """Mock closer if output is stdout"""
        pass

    @abc.abstractmethod
    def write_data(self, file_descr):
        """Expected function that writes into file_descr - object yielded by the open method."""
        pass


class OutputPreProcessing(object):
    """
    Abstract base class used to define how to preprocess the data depending on the category.

    Abstract methods to override:
    property category: string - specifies what category the child is managing (schema, mapping, ...)
    property default_columns: list - name of columns to display in table like outputs
    convert_to_dataframe: convert data into a dataframe - used for list outputs

    Public method that should be overridden:
    columns_values_makers: dict - indicate how to extract data for each columns

    Public method that can be overridden:
    filter_data: preprocess (filter) output data for hierarchical outputs

    Public methods that should not be overridden:
    regularize_column_name: reformat column name to fill columns_values_makers format
    make_column_value: use columns_values_makers to extract column data
    """
    __metaclass__ = abc.ABCMeta

    @property
    @abc.abstractmethod
    def category(self):
        """String - category to manage (schema, mapping, ...)"""
        pass

    @property
    @abc.abstractmethod
    def default_columns(self):
        """List of names of columns to display."""
        pass

    def __new__(cls, category):
        return object.__new__(rec_find_right_subclass(category, attribute='category',
                                                      start_class=cls))

    @classmethod
    def columns_values_makers(cls):
        """{column snake case name: function to extract columns data}"""
        return {}

    @classmethod
    def regularize_column_name(cls, column_name):
        """Column names are stored snake case."""
        return column_name.lower().replace(" ", "_")

    @classmethod
    def make_column_value(cls, column, data, *infos):
        """Call the right function in columns_values_makers to make column value based on infos."""
        column = cls.regularize_column_name(column)
        if column in cls.columns_values_makers():
            return cls.printable_value(cls.columns_values_makers()[column](data, *infos))
        return cls.printable_value(data.get(column))

    @classmethod
    @abc.abstractmethod
    def convert_to_dataframe(cls, data, **kwargs):
        """Create a dataframe from data"""
        return

    @classmethod
    def filter_data(cls, data):
        """Basic method just return data - can be overridden to filter (return json)"""
        return data

    @staticmethod
    def printable_value(value):
        """Helper to manage how to display objects (use json.dumps if possible)"""
        if isinstance(value, (Number, basestring, type(None))):
            return value
        try:
            return json.dumps(value, sort_keys=True)
        except (ValueError, TypeError):
            return str(value)


class _MappingPreProcessing(OutputPreProcessing):
    """Preprocess 'mapping' data from to_sql module"""
    category = 'mapping'
    default_columns = ['Field_name', 'Description', 'Type']

    @classmethod
    def columns_values_makers(cls):
        """How to extract columns data based on field mapping and field name"""
        return {
            # f for field
            'field_name': lambda f_dict, f_name: f_dict['dest'] if 'dest' in f_dict else f_name,
            'type': lambda f_dict, f_name: f_dict['type'],
        }

    @classmethod
    def convert_to_dataframe(cls, data, columns_to_get=None, **kwargs):
        """Transform data (mapping dict) into a dataframe."""
        columns_to_get = columns_to_get or cls.default_columns
        lines = []
        for db in sorted(data):
            for table in sorted(data[db]):
                lines += cls._table_dict_to_lines(db, table, data[db][table], columns_to_get)

        header = ['Database', 'Table'] + columns_to_get
        return pd.DataFrame(lines, columns=header)

    @classmethod
    def _table_dict_to_lines(cls, db_name, table_name, table_dict, columns_to_get):
        """Transform mapping table dict into a list, filtering on columns_to_get."""
        lines = []
        for field_name, field_dict in sorted(table_dict.items(), key=lambda x: x[0]):
            if field_name in ['_id', 'pk']:
                continue
            lines.append([db_name, table_name] +
                         [cls.make_column_value(col_name, field_dict, field_name)
                          for col_name in columns_to_get])
        return lines


class _DiffPreProcessing(OutputPreProcessing):
    """Preprocess 'diff' data from compare module"""
    category = 'diff'
    default_columns = ['Hierarchy', 'Previous Schema', 'New Schema']

    @classmethod
    def columns_values_makers(cls):
        """How to extract columns data based on diff dict and hierarchy"""
        return {
            'hierarchy': lambda diff, hierarchy: '.'.join(hierarchy),
            'previous_schema': lambda diff, hierarchy: diff['prev_schema']
        }

    @classmethod
    def convert_to_dataframe(cls, data, columns_to_get=None, **kwargs):
        """Transform data (list of dicts) into a dataframe."""
        columns_to_get = columns_to_get or cls.default_columns

        table = []
        for d in data:
            hierarchy = d['hierarchy'].split('.')
            db = hierarchy.pop(0)
            coll = hierarchy.pop(0) if hierarchy else ''

            table.append([db, coll] +
                         [cls.make_column_value(col_name, d, hierarchy)
                          for col_name in columns_to_get])

        header = ['Database', 'Collection'] + columns_to_get
        return pd.DataFrame(table, columns=header)


class _SchemaPreProcessing(OutputPreProcessing):
    """Prepocess mongo schema"""
    category = 'schema'
    default_columns = ['Field_full_name', 'Depth', 'Field_name', 'Type']

    @classmethod
    def columns_values_makers(cls):
        """How to extract columns data based on field schema, name and prefix."""
        return {  # 'f' for field
            'field_full_name': lambda f_schema, f, f_prefix: f_prefix + f,
            'field_compact_name': cls._field_compact_name,
            'field_name': lambda f_schema, f, f_prefix: f,
            'depth': cls._field_depth,
            'type': cls._field_type,
            'percentage': lambda f_schema, f, f_prefix: (100 * f_schema['prop_in_object']
                                                         if 'prop_in_object' in f_schema else None),
            'types_count': lambda f_schema, f, f_prefix: cls._format_types_count(
                f_schema.get('types_count', None), f_schema.get('array_types_count', None)),
        }

    @classmethod
    def filter_data(cls, data):
        """ Recursively copy schema without count fields.

        :param data: schema or subpart of schema
        :return dict (subpart of schema without count fields) or original value
        """
        if isinstance(data, dict):
            schema_filtered = dict()
            for k, v in data.items():
                if k not in ['count', 'types_count', 'prop_in_object', 'array_types_count']:
                    schema_filtered[k] = cls.filter_data(v)
            return schema_filtered
        return data

    @classmethod
    def convert_to_dataframe(cls, data, columns_to_get=None, **kwargs):
        """
        Load schema (data) into dataframe, filtering on columns_to_get (column names list).
        """
        columns_to_get = columns_to_get or cls.default_columns
        line_tuples = list()
        for database, database_schema in sorted(list(data.items())):
            for collection, collection_schema in sorted(list(database_schema.items())):
                collection_line_tuples = cls._object_schema_to_line_tuples(
                    collection_schema['object'], columns_to_get, field_prefix='')
                for line in collection_line_tuples:
                    line_tuples.append([database, collection] + list(line))

        header = tuple(['Database', 'Collection'] + columns_to_get)
        return pd.DataFrame(line_tuples, columns=header)

    @classmethod
    def _object_schema_to_line_tuples(cls, object_schema, columns_to_get, field_prefix):
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
        sorted_fields = sorted(list(object_schema.items()),
                               key=lambda x: (-x[1]['count'], x[0]) if 'count' in x[1] else x[0])

        for field, field_schema in sorted_fields:
            line_columns = cls._field_schema_to_columns(
                field, field_schema, field_prefix, columns_to_get)
            line_tuples.append(line_columns)

            types = field_schema.get('types_count', [field_schema['type']])

            if 'object' in field_schema:
                if 'ARRAY' in types:
                    current_prefix = field_prefix + field + ':'
                elif 'OBJECT' in types:
                    current_prefix = field_prefix + field + '.'
                else:
                    logger.warning('Field {} has key "object" but has types {} while should have '
                                   '"OBJECT" or "ARRAY"'.format(field, types))
                    continue
                line_tuples += cls._object_schema_to_line_tuples(
                    field_schema['object'], columns_to_get, field_prefix=current_prefix)

        return line_tuples

    @classmethod
    def _field_schema_to_columns(cls, field_name, field_schema, field_prefix, columns_to_get):
        """ Given fields information, returns a tuple representing columns_to_get.

        :param field_name:
        :param field_schema:
        :param field_prefix: str, default ''
        :param columns_to_get: iterable
            columns to create for each field
        :return field_columns: tuple
        """
        field_columns = list()
        for column in columns_to_get:
            field_columns.append(cls.make_column_value(column, field_schema, field_name, field_prefix))

        return tuple(field_columns)

    @staticmethod
    def _field_compact_name(field_schema, field_name, field_prefix):
        """ Return a compact version of field name, without parent object names.

        >>> field_compact_name(None, 'baz', 'foo.bar:')
        " .  : baz"
        """
        separators = re.sub('[^.:]', '', field_prefix)
        separators = re.sub('\.', ' . ', separators)
        separators = re.sub(':', ' : ', separators)
        return separators + field_name

    @staticmethod
    def _field_depth(field_schema, field_name, field_prefix):
        """ Return the level of imbrication of a field."""
        separators = re.sub('[^.:]', '', field_prefix)
        return len(separators)

    @staticmethod
    def _field_type(field_schema, field_name, field_prefix):
        """ Return a string describing the type of a field."""
        f_type = field_schema['type']
        if f_type == 'ARRAY':
            f_type = 'ARRAY(' + field_schema['array_type'] + ')'
        return f_type

    @staticmethod
    def _format_types_count(types_count, array_types_count=None):
        """ Format types_count to a readable sting.

        >>> format_types_count({'integer': 10, 'boolean': 5, 'null': 3, })
        'integer : 10, boolean : 5, null : 3'

        >>> format_types_count({'ARRAY': 10, 'null': 3, }, {'float': 4})
        'ARRAY(float : 4) : 10, null : 3'

        :param types_count: dict
        :param array_types_count: dict, default None
        :return types_count_string : str
        """
        if types_count is None:
            return str(None)

        types_count = sorted(types_count.items(), key=lambda x: x[1], reverse=True)

        type_count_list = list()
        for type_name, count in types_count:
            if type_name == 'ARRAY':
                array_type_name = _SchemaPreProcessing._format_types_count(array_types_count)
                type_count_list.append('ARRAY(' + array_type_name + ') : ' + str(count))
            else:
                type_count_list.append(str(type_name) + ' : ' + str(count))

        types_count_string = ', '.join(type_count_list)
        return types_count_string


class HierarchicalOutput(BaseOutput):
    """
    Abstract base class. Preprocessing for outputs that keep the json hierarchical structure.
    """

    def __init__(self, data, category='schema', without_counts=False, **kwargs):
        """
        :param data: json like structure - schema, mapping, ...
        :param without_counts: bool - default False, remove all count fields in output if True
        :param kwargs: unused - exists for a unified interface with other subclasses of BaseOutput
        """
        data_processor = OutputPreProcessing(category)
        if without_counts:
            self.data = data_processor.filter_data(data)
        else:
            self.data = data


class ListOutput(BaseOutput):
    """
    Abstract base class. Preprocessing for outputs with a table like format.

    Class attribute:
    _default_columns: allow to override PreProcessing class default_columns
                        {category: [default_columns]}
    """
    _default_columns = {}

    @classmethod
    def get_default_columns(cls):
        """List default columns by category"""
        return {
            'schema': cls._default_columns.get('schema', _SchemaPreProcessing.default_columns),
            'mapping': cls._default_columns.get('mapping', _MappingPreProcessing.default_columns),
            'diff': cls._default_columns.get('diff', _DiffPreProcessing.default_columns)}

    def __init__(self, data, category='schema', columns_to_get=None, **kwargs):
        """
        :param data: json like structure - schema, mapping, ...
        :param columns_to_get: list - column names to display in output
                                default will use default_columns class attribute
        :param kwargs: unused - exists for a unified interface with other subclasses of BaseOutput
        """
        data_processor = OutputPreProcessing(category)
        if not columns_to_get:
            columns_to_get = self.get_default_columns()[category]

        self.data_df = data_processor.convert_to_dataframe(data, columns_to_get=columns_to_get)


class JsonOutput(HierarchicalOutput):
    """
    Write data in json file.
    """
    output_format = 'json'

    def opener(self):
        """Use codecs module open function to support non ascii characters."""
        return partial(codecs.open, mode='w', encoding="utf-8")

    def write_data(self, file_descr):
        """Use json module dump function to write into file_descr (opened with opener)."""
        json.dump(self.data, file_descr, indent=4, ensure_ascii=False,
                  default=json_util.default, sort_keys=True)


class YamlOutput(HierarchicalOutput):
    """
    Write data in yaml file.
    """
    output_format = 'yaml'

    def write_data(self, file_descr):
        """Use yaml module safe_dump function to write into file_descr."""
        yaml.safe_dump(self.data, file_descr, default_flow_style=False, encoding='utf-8')


class TsvOutput(ListOutput):
    """
    Write data from self.data_df as a table in csv file.
    """
    output_format = 'tsv'

    def write_data(self, file_descr):
        """Use dataframe to_csv method to write into file_descr."""
        self.data_df.to_csv(file_descr, sep='\t', index=False, encoding="utf-8",
                            quoting=csv.QUOTE_NONE)


class HtmlOutput(ListOutput):
    """
    Write data from self.data_df as a table in html file, one table per collection.

    Uses resources/data_dict.tmpl template.
    """
    output_format = 'html'
    _default_columns = {
        'schema': ['Field_compact_name', 'Field_name', 'Count',
                   'Percentage', 'Types_count']}

    def opener(self):
        """Use codecs module open function to support non ascii characters."""
        return partial(codecs.open, mode='w', encoding="utf-8")

    def write_data(self, file_descr):
        """
        Format data from self.data_df, write into file_descr (opened with opener).
        """
        columns = self.data_df.columns
        col0 = columns[0]  # First column title (usually Database)
        col1 = columns[1]  # Second column title (usually Collection or Table)
        tmpl_variables = OrderedDict()
        for db in self.data_df[col0].unique():
            tmpl_variables[db] = OrderedDict()
            df_db = self.data_df.query('{} == @db'.format(col0)).iloc[:, 1:]
            for col in df_db[col1].unique():
                df_col = df_db.query('{} == @col'.format(col1)).iloc[:, 1:]
                tmpl_variables[db][col] = df_col.values.tolist()

        tmpl_filename = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                     'resources', 'data_dict.tmpl')
        with open(tmpl_filename) as tmpl_fd:
            tmpl = jinja2.Template(tmpl_fd.read())

        file_descr.write(tmpl.render(col_titles=list(self.data_df)[2:],
                                     data=tmpl_variables))


class MdOutput(ListOutput):
    """
    Write data from self.data_df as a table in markdown file, one table per Collection.
    """
    output_format = 'md'
    _default_columns = {
        'schema': ['Field_compact_name', 'Field_name', 'Count',
                   'Percentage', 'Types_count']}

    def opener(self):
        """Use codecs module open function to support non ascii characters."""
        return partial(codecs.open, mode='w', encoding="utf-8")

    def write_data(self, file_descr):
        """
        Format data from self.data_df, write into file_descr (opened with opener).
        """
        columns = list(self.data_df.columns)
        col0 = columns.pop(0)       # First column title (usually Database)
        col1 = columns.pop(0)       # Second column title (usually Collection or Table)
        columns_length = []
        for col in columns:
            columns_length.append(max(self.data_df[col].map(
                lambda s: len(s) if isinstance(s, basestring) else len(str(s))).max(),
                                      len(col)) + 5)

        def format_column(col_name, value, repeat=False):
            """Closure - format columns based on existing data length."""
            col_length = columns_length[columns.index(col_name)]
            if repeat:
                return value * col_length
            return u'{{:<{}}}'.format(col_length).format(u'{}'.format(value))

        str_column_names = self._make_line([format_column(col, col) for col in columns])
        str_sep_header = self._make_line([format_column(col, '-', repeat=True) for col in columns])
        output_str = []
        for db in self.data_df[col0].unique():
            output_str.append('\n### {}: {}\n'.format(col0, db))
            df_db = self.data_df.query('{} == @db'.format(col0)).iloc[:, 1:]
            for col in df_db[col1].unique():
                if col:
                    output_str.append('#### {}: {} \n'.format(col1, col))
                df_col = df_db.query('{} == @col'.format(col1)).iloc[:, 1:]
                output_str.append("\n".join([str_column_names, str_sep_header] +
                                            [self._make_line([format_column(columns[i], value)
                                                              for i, value in enumerate(line)])
                                             for line in df_col.values.tolist()]))
                output_str.append('\n\n')

        file_descr.write("".join(output_str))

    def _make_line(self, values):
        return u'|{}|'.format('|'.join(values))


class XlsxOutput(ListOutput):
    """
    Write data from self.data_df as a table in csv file.
    """
    output_format = 'xlsx'

    def opener(self):
        """
        Just return filename.

        Write_data will manage the opening based on whether the file already exists
        """
        return lambda x: x

    def write_data(self, file_descr):
        """
        Use dataframe to_excel to write into file_descr (filename) - open first if file exists.
        """
        if os.path.isfile(file_descr):
            print(file_descr, 'exists')
            # Solution to keep existing data
            book = load_workbook(file_descr)
            writer = pd.ExcelWriter(file_descr, engine='openpyxl')
            writer.book = book
            writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
            self.data_df.to_excel(writer, sheet_name='Mongo_Schema', index=True,
                                  float_format='%.2f')
            writer.save()
        else:
            self.data_df.to_excel(file_descr, sheet_name='Mongo_Schema', index=True,
                                  float_format='%.2f')


def rec_find_right_subclass(attribute_value, attribute='output_format', start_class=BaseOutput):
    """Find which subclass of start_class should be used (has the right attribute value)"""
    for subclass in start_class.__subclasses__():
        if getattr(subclass, attribute) == attribute_value:
            return subclass
        rec_res = rec_find_right_subclass(attribute_value, start_class=subclass)
        if rec_res:
            return rec_res

    return None


def transform_data_to_file(data, formats, output=None, category='schema', **kwargs):
    """
    Transform data into each of output_formats and write result to output_filename or stdout.

    :param data: dict (schema, mapping or diff)
    :param formats: list of str - extensions of output desired among:
                            'json', 'yaml' (hierarchical formats)
                            'tsv', 'html', 'md' or 'xlsx' (list like formats)
    :param output: str full path to file where formatted output will be saved saved
                            (default is std out)
    :param category: string in 'schema', 'mapping', 'diff' - describe input data
    :param kwargs: may contain additional specific arguments
           columns: list of columns to display in the output for list like formats
           without_counts: bool to display count fields in output for hierarchical formats
    """
    wrong_formats = set(formats) - {'tsv', 'xlsx', 'json', 'yaml', 'html', 'md'}

    if wrong_formats:
        raise ValueError("Output format should be tsv, xlsx, html, md, json or yaml. "
                         "{} is/are not supported".format(wrong_formats))

    for output_format in formats:
        output_maker = rec_find_right_subclass(output_format)(
            data, category=category,
            columns_to_get=kwargs.get('columns'), without_counts=kwargs.get('without_counts'))
        with output_maker.open(output) as file_descr:
            output_maker.write_data(file_descr)
