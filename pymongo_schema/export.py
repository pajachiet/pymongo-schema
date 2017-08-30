# coding: utf8
"""
This module intends to transform mongo schema or mapping to write them in different file types.

Entry point is write_output_dict.

It relies on a class system that defines how to open and write into the different file types.

The base class is BaseOutput, it defines the mechanism.
abstract property : output_format
abstract method: write_output_data
Those must be overridden in the final subclasses.
The opener method should be overridden as well if open method is not enough
(to manage non ascii for example).

It is inherited by two base classes, that represent two groups of outputs:
HierarchicalOutput for nested formats (yaml and json)
ListOutput for table like formats (txt, csv, md, html - since this format displays a table, xlsx).
They intend to setup prepare data as this is common to each group of output.
ListOutput defines a default_columns class attribute that can be overridden as well.

Then those base classes are used (inherited from) to define each format:
JsonOutput, YamlOutput, TxtOutput, CsvOutput, HtmlOutput, MdOutput, XlsxOutput
"""
import abc
import codecs
import json
import logging
import os
import re
import sys
from contextlib import contextmanager
from functools import partial

import yaml
import jinja2
from future.moves.collections import OrderedDict
from past.builtins import basestring
import pandas as pd
from openpyxl import load_workbook

logger = logging.getLogger(__name__)


class BaseOutput(object):
    """
    Abstract base class. Defines the mechanism allowing to write schema into various file formats.

    The principle is to be able to define how to open the file and how to write into it
    using the same interface no matter what format is asked.

    Abstract methods to override:
    property output_format (can be class attribute): string representing the format expected
        ('json', 'yaml', 'txt', 'csv', 'html', 'md, 'xlsx', ... it should match file extension)
    write_output_data: define how to write into the object given by the open method

    Other public method (should not be overridden):
    open: context manager that yields the file ready to be written into by write_output_data.
        It uses private methods _opener and _closer to define how to open and close the file

    Private methods that can be overridden to specialize the output:
    _opener
    _closer
    """
    __metaclass__ = abc.ABCMeta

    @property
    @abc.abstractmethod
    def output_format(self):
        """Expected string matching the extension of the file format."""
        pass

    @contextmanager
    def open(self, filename):
        """Yields a file descriptor as expected in self.write_output_data"""
        if not filename:  # output is stdout, opener and closer must be adapted
            self._opener = self._stdout_opener
            self._closer = self._stdout_closer
        else:
            if not filename.endswith('.' + self.output_format):  # Add extension
                filename += '.' + self.output_format
        file_descr = self._opener()(filename)
        try:
            yield file_descr
        finally:
            self._closer(file_descr)

    def _opener(self):
        """Return the function used to open the file (only filename will be passed as argument)."""
        return partial(open, mode='w')

    def _stdout_opener(self):
        """Mock _opener if output is stdout"""
        return lambda x: sys.stdout

    def _closer(self, file_descr):
        """Actions to perform to close the file."""
        try:
            file_descr.close()
        except AttributeError:
            pass

    def _stdout_closer(self, file_descr):
        """Mock _closer if output is stdout"""
        pass

    @abc.abstractmethod
    def write_output_data(self, file_descr):
        """Expected function that writes into file_descr - object yielded by the open method."""
        pass


class OutputPreProcessing(object):
    __metaclass__ = abc.ABCMeta

    @property
    @abc.abstractmethod
    def category(self):
        """"""
        pass

    def __new__(cls, category):
        o = object.__new__(rec_find_right_subclass(category, attribute='category', start_class=cls))
        return o

    @classmethod
    @abc.abstractmethod
    def mongo_schema_as_dataframe(cls, output_data, columns_to_get=None):
        return

    def remove_counts_from_schema(self, schema):
        return schema


class _SchemaPreProcessing(OutputPreProcessing):
    category = 'schema'

    @classmethod
    def remove_counts_from_schema(cls, value):
        """ Recursively copy schema without count fields.

        :param value: schema or subpart of schema
        :return dict (subpart of schema without count fields) or original value
        """
        if isinstance(value, dict):
            schema_filtered = dict()
            for k, v in value.items():
                if k not in ['count', 'types_count', 'prop_in_object', 'array_types_count']:
                    schema_filtered[k] = cls.remove_counts_from_schema(v)
            return schema_filtered
        return value

    @classmethod
    def mongo_schema_as_dataframe(cls, output_data, columns_to_get=None):
        """
        Load schema (output_data) into dataframe, filtering on columns_to_get (column names list).
        """
        line_tuples = list()
        for database, database_schema in sorted(list(output_data.items())):
            for collection, collection_schema in sorted(list(database_schema.items())):
                collection_line_tuples = cls._object_schema_to_line_tuples(
                    collection_schema['object'], columns_to_get, field_prefix='')
                for line in collection_line_tuples:
                    line_tuples.append([database, collection] + list(line))

        header = tuple(['Database', 'Collection'] + columns_to_get)
        mongo_schema_df = pd.DataFrame(line_tuples, columns=header)
        return mongo_schema_df

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
        sorted_fields = sorted(list(object_schema.items()), key=lambda x: (-x[1]['count'], x[0]))

        for field, field_schema in sorted_fields:
            line_columns = cls._field_schema_to_columns(
                field, field_schema, field_prefix, columns_to_get)
            line_tuples.append(line_columns)

            if 'ARRAY' in field_schema['types_count'] \
                    and 'OBJECT' in field_schema['array_types_count']:
                line_tuples += cls._object_schema_to_line_tuples(
                    field_schema['object'], columns_to_get, field_prefix=field_prefix + field + ':')

            elif 'OBJECT' in field_schema['types_count']:
                # 'elif' rather than 'if' in case of both OBJECT and ARRAY(OBJECT)
                line_tuples += cls._object_schema_to_line_tuples(
                    field_schema['object'], columns_to_get, field_prefix=field_prefix + field + '.')

        return line_tuples

    @classmethod
    def _field_schema_to_columns(cls, field, field_schema, field_prefix, columns_to_get):
        """ Given fields information, returns a tuple representing columns_to_get.

        :param field:
        :param field_schema:
        :param field_prefix: str, default ''
        :param columns_to_get: iterable
            columns to create for each field
        :return field_columns: tuple
        """
        # 'f' for field
        column_functions = {
            'field_full_name': lambda f, f_schema, f_prefix: f_prefix + f,
            'field_compact_name': cls._field_compact_name,
            'field_name': lambda f, f_schema, f_prefix: f,
            'depth': cls._field_depth,
            'type': cls._field_type,
            'percentage': lambda f, f_schema, f_prefix: 100 * f_schema['prop_in_object'],
            'types_count': lambda f, f_schema, f_prefix: cls._format_types_count(
                f_schema['types_count'], f_schema.get('array_types_count', None)),
        }

        field_columns = list()
        for column in columns_to_get:
            column = column.lower()
            if column not in column_functions:
                column_str = field_schema.get(column, None)
            else:
                column_str = column_functions[column](field, field_schema, field_prefix)
            field_columns.append(column_str)

        return tuple(field_columns)

    @classmethod
    def _field_compact_name(cls, field, field_schema, field_prefix):
        """ Return a compact version of field name, without parent object names.

        >>> field_compact_name('baz', None, 'foo.bar:')
        " .  : baz"
        """
        separators = re.sub('[^.:]', '', field_prefix)
        separators = re.sub('\.', ' . ', separators)
        separators = re.sub(':', ' : ', separators)
        return separators + field

    @classmethod
    def _field_depth(cls, field, field_schema, field_prefix):
        """ Return the level of imbrication of a field."""
        separators = re.sub('[^.:]', '', field_prefix)
        return len(separators)

    @classmethod
    def _field_type(cls, field, field_schema, field_prefix):
        """ Return a string describing the type of a field."""
        f_type = field_schema['type']
        if f_type == 'ARRAY':
            f_type = 'ARRAY(' + field_schema['array_type'] + ')'
        return f_type

    @classmethod
    def _format_types_count(cls, types_count, array_types_count=None):
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
                array_type_name = cls._format_types_count(array_types_count)
                type_count_list.append('ARRAY(' + array_type_name + ') : ' + str(count))
            else:
                type_count_list.append(str(type_name) + ' : ' + str(count))

        types_count_string = ', '.join(type_count_list)
        return types_count_string


class HierarchicalOutput(BaseOutput):
    """
    Abstract base class. Preprocessing for outputs that keep the json hierarchical structure.
    """

    def __init__(self, output_data, category='schema', without_counts=False, **kwargs):    # TODO: output_data -> data ?
        """
        :param output_data: dict - schema
        :param without_counts: bool - default False, remove all count fields in output if True
        :param kwargs: unused - exists for a unified interface with other subclasses of BaseOutput
        """
        data_processor = OutputPreProcessing(category)
        if without_counts:
            self.output_data = data_processor.remove_counts_from_schema(output_data)
        else:
            self.output_data = output_data


class ListOutput(BaseOutput):
    """
    Abstract base class. Preprocessing for outputs with a table like format.
    """
    default_columns = ['Field_full_name', 'Depth', 'Field_name', 'Type']

    def __init__(self, output_data, category='schema', columns_to_get=None, **kwargs):
        """
        :param output_data: dict - schema
        :param columns_to_get: string of column names to display in output separated by spaces
                                default will use default_columns class attribute
        :param kwargs: unused - exists for a unified interface with other subclasses of BaseOutput
        """
        data_processor = OutputPreProcessing(category)
        self.mongo_schema_df = data_processor.mongo_schema_as_dataframe(
            output_data,
            columns_to_get.split(" ") if columns_to_get else self.default_columns)


class JsonOutput(HierarchicalOutput):
    """
    Write data in json file.
    """
    output_format = 'json'

    def _opener(self):
        """Use codecs module open function to support non ascii characters."""
        return partial(codecs.open, mode='w', encoding="utf-8")

    def write_output_data(self, file_descr):
        """Use json module dump function to write into file_descr (opened with _opener)."""
        json.dump(self.output_data, file_descr, indent=4, ensure_ascii=False)


class YamlOutput(HierarchicalOutput):
    """
    Write data in yaml file.
    """
    output_format = 'yaml'

    def write_output_data(self, file_descr):
        """Use yaml module safe_dump function to write into file_descr."""
        yaml.safe_dump(self.output_data, file_descr, default_flow_style=False, encoding='utf-8')


class TxtOutput(ListOutput):
    """
    Write data from self.mongo_schema_df as a table in text file, one table per Collection.
    """
    output_format = 'txt'
    default_columns = ['Field_compact_name', 'Field_name', 'Count', 'Percentage', 'Types_count']

    def _opener(self):
        """Use codecs module open function to support non ascii characters."""
        return partial(codecs.open, mode='w', encoding="utf-8")

    def write_output_data(self, file_descr):
        """
        Format data from self.mongo_schema_df, write into file_descr (opened with _opener).
        """
        pd.options.display.max_colwidth = 1000
        formatters = dict()
        for col in self.mongo_schema_df.columns:
            col_len = self.mongo_schema_df[col].map(
                lambda s: len(s) if isinstance(s, basestring) else len(str(s))).max()
            formatters[col] = u'{{:<{}}}'.format(col_len + 3).format

        output_str = ''
        for db in self.mongo_schema_df.Database.unique():
            output_str += '\n### Database: {}\n'.format(db)
            df_db = self.mongo_schema_df.query('Database == @db').iloc[:, 1:]
            for col in df_db.Collection.unique():
                if col:
                    output_str += '--- Collection: {} \n'.format(col)
                df_col = df_db.query('Collection == @col').iloc[:, 1:]
                output_str += df_col.to_string(index=False, formatters=formatters, justify='left',
                                               float_format=lambda x: '%.2f' % x)
                output_str += '\n\n'

        file_descr.write(output_str)


class CsvOutput(ListOutput):
    """
    Write data from self.mongo_schema_df as a table in csv file.
    """
    output_format = 'csv'

    def write_output_data(self, file_descr):
        """Use dataframe to_csv method to write into file_descr."""
        self.mongo_schema_df.to_csv(file_descr, sep='\t', index=False, encoding="utf-8")


class HtmlOutput(ListOutput):
    """
    Write data from self.mongo_schema_df as a table in html file, one table per collection.

    Uses resources/data_dict.tmpl template.
    """
    output_format = 'html'
    default_columns = ['Field_compact_name', 'Field_name', 'Full_name', 'Description', 'Count',
                       'Percentage', 'Types_count']

    def _opener(self):
        """Use codecs module open function to support non ascii characters."""
        return partial(codecs.open, mode='w', encoding="utf-8")

    def write_output_data(self, file_descr):
        """
        Format data from self.mongo_schema_df, write into file_descr (opened with _opener).
        """
        mongo_schema_tmpl = OrderedDict()
        for db in self.mongo_schema_df.Database.unique():
            mongo_schema_tmpl[db] = OrderedDict()
            df_db = self.mongo_schema_df.query('Database == @db').iloc[:, 1:]
            for col in df_db.Collection.unique():
                df_col = df_db.query('Collection == @col').iloc[:, 1:]
                mongo_schema_tmpl[db][col] = df_col.values.tolist()

        tmpl_filename = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                     'resources', 'data_dict.tmpl')
        with open(tmpl_filename) as tmpl_fd:
            tmpl = jinja2.Template(tmpl_fd.read())

        file_descr.write(tmpl.render(col_titles=list(self.mongo_schema_df)[2:],
                                     mongo_schema=mongo_schema_tmpl))


class MdOutput(ListOutput):
    """
    Write data from self.mongo_schema_df as a table in markdown file, one table per Collection.
    """
    output_format = 'md'
    default_columns = ['Field_compact_name', 'Field_name', 'Full_name', 'Description', 'Count',
                       'Percentage', 'Types_count']

    def _opener(self):
        """Use codecs module open function to support non ascii characters."""
        return partial(codecs.open, mode='w', encoding="utf-8")

    def write_output_data(self, file_descr):
        """
        Format data from self.mongo_schema_df, write into file_descr (opened with _opener).
        """
        columns = list(self.mongo_schema_df.columns)[2:]  # skip Database and Collection
        columns_length = []
        for col in columns:
            columns_length.append(max(self.mongo_schema_df[col].map(
                lambda s: len(s) if isinstance(s, basestring) else len(str(s))).max(),
                                      len(col)) + 5)

        def format_column(col_name, value, repeat=False):
            """Closure - format columns based on existing data length."""
            col_length = columns_length[columns.index(col_name)]
            if repeat:
                return value * col_length
            return u'{{:<{}}}'.format(col_length).format(value if value is not None else str(value))

        str_column_names = self._make_line([format_column(col, col) for col in columns])
        str_sep_header = self._make_line([format_column(col, '-', repeat=True) for col in columns])
        output_str = []
        for db in self.mongo_schema_df.Database.unique():
            output_str.append('\n### Database: {}\n'.format(db))
            df_db = self.mongo_schema_df.query('Database == @db').iloc[:, 1:]
            for col in df_db.Collection.unique():
                if col:
                    output_str.append('#### Collection: {} \n'.format(col))
                df_col = df_db.query('Collection == @col').iloc[:, 1:]
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
    Write data from self.mongo_schema_df as a table in csv file.
    """
    output_format = 'xlsx'

    def _opener(self):
        """
        Just return filename.

        Write_output_data will manage the opening based on whether the file already exists
        """
        return lambda x: x

    def write_output_data(self, file_descr):
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
            self.mongo_schema_df.to_excel(writer, sheet_name='Mongo_Schema', index=True,
                                          float_format='%.2f')
            writer.save()
        else:
            self.mongo_schema_df.to_excel(file_descr, sheet_name='Mongo_Schema', index=True,
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


def write_output_dict(output_dict, arg):
    """
    Write output dictionary to file or standard output, with specific format described in arg
    :param output_dict: dict (schema or mapping)
    :param arg: dict (from docopt)
           if output_dict is schema
               {'--format': str in 'json', 'yaml', 'txt', 'csv', 'html', 'md' or 'xlsx',
                '--output': str full path to file where formatted output will be saved saved
                            (default is std out),
                '--columns': list of columns to display in the output not used for json and yaml}
           if output_dict is mapping
               {'--format': str in 'json', 'yaml',
                '--output': same as for schema (path to file where output will be saved saved),
                '__columns': unused but key must exist,
                '--without-counts': bool to display counts in output}
    """
    output_formats = arg['--format']
    output_filename = arg['--output']
    columns_to_get = arg.get('--columns', None)
    without_counts = arg.get('--without-counts', False)

    wrong_formats = set(output_formats) - {'txt', 'csv', 'xlsx', 'json', 'yaml', 'html', 'md'}

    if wrong_formats:
        raise ValueError("Output format should be txt, csv, xlsx, html, md, json or yaml. "
                         "{} is/are not supported".format(wrong_formats))

    for output_format in output_formats:
        output_maker = rec_find_right_subclass(output_format)(output_dict,
                                                              columns_to_get=columns_to_get,
                                                              without_counts=without_counts)
        with output_maker.open(output_filename) as file_descr:
            output_maker.write_output_data(file_descr)
