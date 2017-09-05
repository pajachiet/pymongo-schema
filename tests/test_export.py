# coding: utf8
import pytest
import filecmp
from pandas.util.testing import assert_frame_equal

from pymongo_schema.export import *

TEST_DIR = os.path.dirname(__file__)


@pytest.fixture(scope='module')
def simple_schema():
    return {'count': 25359,
            'object': {'field': {'types_count': {'string': 25359},
                                 'count': 25359, 'type': 'string',
                                 'prop_in_object': 1.0}}}


@pytest.fixture(scope='module')
def long_schema():
    return {
        'count': 25359,
        'object': {'field': {'types_count': {'string': 25359}, 'count': 25359, 'type': 'string',
                             'prop_in_object': 1.0},
                   'field2': {'types_count': {'string': 25359}, 'count': 25359,
                              'type': 'string',
                              'prop_in_object': 1.0},
                   'field3': {'types_count': {'ARRAY': 25359}, 'count': 25359, 'type': 'ARRAY',
                              'prop_in_object': 1.0, 'array_types_count': {'OBJECT': 25359},
                              'array_type': 'OBJECT',
                              'object': {'subfield1': {'types_count': {'string': 25359},
                                                       'count': 25359, 'type': 'string',
                                                       'prop_in_object': 1.0},
                                         'subfield2': {'types_count': {'object': 25359},
                                                       'count': 25359, 'type': 'ARRAY',
                                                       'prop_in_object': 1.0,
                                                       'array_type': 'string',
                                                       'array_types_count': {'string': 93463,
                                                                             'null': 738}}}}}}


@pytest.fixture(scope='module')
def simple_full_schema(simple_schema):
    return {'db': {'coll': simple_schema}}


@pytest.fixture(scope='module')
def long_full_schema(simple_schema, long_schema):
    return {'db1': {'coll': long_schema},
            'db2': {'coll1': simple_schema, 'coll2': simple_schema}}


@pytest.fixture(scope='module')
def mapping_ex_dict():
    with open(os.path.join(TEST_DIR, 'resources', 'input', 'mapping.json')) as f:
        return json.load(f)


@pytest.fixture(scope='module')
def schema_ex_dict():
    with open(os.path.join(TEST_DIR, 'resources', 'input', 'test_schema.json')) as f:
        return json.load(f)


@pytest.fixture(scope='module')
def columns():
    return ['Field_compact_name', 'Field_name', 'Full_name', 'Description', 'Count',
            'Percentage', 'Types_count']


@pytest.fixture(scope='module')
def schema_ex_df(schema_ex_dict, columns):
    return ListOutput.mongo_schema_as_dataframe(schema_ex_dict, columns)


def test00_field_compact_name():
    assert ListOutput._field_compact_name('baz', None, 'foo.bar:') == ' .  : baz'
    assert ListOutput._field_compact_name('baz', None, 'foo.foo.bar:') == ' .  .  : baz'


def test01_field_depth():
    assert ListOutput._field_depth(None, None, '') == 0
    assert ListOutput._field_depth(None, None, 'foo') == 0
    assert ListOutput._field_depth(None, None, 'foo.bar') == 1
    assert ListOutput._field_depth(None, None, 'foo.bar:') == 2
    assert ListOutput._field_depth(None, None, 'foo.foo.bar:') == 3


def test02_field_type():
    assert ListOutput._field_type(None, {'type': 'string'}, None) == 'string'
    assert ListOutput._field_type(
        None, {'type': 'ARRAY', 'array_type': 'string'}, None) == 'ARRAY(string)'


def test03_format_types_count():
    res = ListOutput._format_types_count({'integer': 10, 'boolean': 5, 'null': 3, })
    assert res == 'integer : 10, boolean : 5, null : 3'
    res = ListOutput._format_types_count({'ARRAY': 10, 'null': 3, }, {'float': 4})
    assert res == 'ARRAY(float : 4) : 10, null : 3'


def test04_remove_counts_from_schema_simple(simple_schema):
    exp = {'object': {'field': {'type': 'string'}}}
    assert HierarchicalOutput.remove_counts_from_schema(simple_schema) == exp


def test05_remove_counts_from_schema_empty():
    assert HierarchicalOutput.remove_counts_from_schema({}) == {}


def test06_remove_counts_from_schema_long(long_schema):
    exp = {'object': {'field': {'type': 'string'},
                      'field2': {'type': 'string'},
                      'field3': {'type': 'ARRAY', 'array_type': 'OBJECT',
                                 'object': {'subfield1': {'type': 'string'},
                                            'subfield2': {'type': 'ARRAY',
                                                          'array_type': 'string'}}}}}
    assert HierarchicalOutput.remove_counts_from_schema(long_schema) == exp


def test07_field_schema_to_columns_simple(simple_schema):
    res = ListOutput._field_schema_to_columns(
        'field', simple_schema['object']['field'], '',
        ['Field_compact_name', 'Field_name', 'Count', 'Percentage', 'Types_count'])
    assert res == ('field', 'field', 25359, 100.0, 'string : 25359')


def test08_field_schema_to_columns_long(long_schema):
    res = ListOutput._field_schema_to_columns(
        'field3', long_schema['object']['field3'], 'foo.bar:',
        ['Field_full_name', 'Field_compact_name', 'Field_name', 'Depth', 'Type', 'Count',
         'Percentage', 'Types_count'])
    exp = ('foo.bar:field3', ' .  : field3', 'field3', 2, 'ARRAY(OBJECT)', 25359, 100.0,
           'ARRAY(OBJECT : 25359) : 25359')
    assert res == exp

def test09_object_schema_to_line_tuples_simple(simple_schema):
    res = ListOutput._object_schema_to_line_tuples(
        simple_schema['object'], ['Field_compact_name', 'Types_count'], '')
    assert res == [('field', 'string : 25359')]


def test10_object_schema_to_line_tuples_long(long_schema):
    res = ListOutput._object_schema_to_line_tuples(
        long_schema['object'],
        ['Field_full_name', 'Field_compact_name', 'Field_name', 'Type', 'Count', 'Types_count'],
        'foo.bar:')
    exp = [
        ('foo.bar:field', ' .  : field', 'field', 'string', 25359, 'string : 25359'),
        ('foo.bar:field2', ' .  : field2', 'field2', 'string', 25359, 'string : 25359'),
        ('foo.bar:field3', ' .  : field3', 'field3', 'ARRAY(OBJECT)', 25359,
         'ARRAY(OBJECT : 25359) : 25359'),
        ('foo.bar:field3:subfield1', ' .  :  : subfield1', 'subfield1', 'string', 25359,
         'string : 25359'),
        ('foo.bar:field3:subfield2', ' .  :  : subfield2', 'subfield2', 'ARRAY(string)',
         25359, 'object : 25359')]
    assert res == exp


def test11_mongo_schema_as_dataframe_simple(simple_full_schema):
    columns = ['Field_compact_name', 'Types_count']
    res = ListOutput.mongo_schema_as_dataframe(simple_full_schema, columns)
    exp = pd.DataFrame([['db', 'coll', 'field', 'string : 25359']],
                       columns=['Database', 'Collection'] + columns)
    assert_frame_equal(res, exp)


def test12_mongo_schema_as_dataframe_long(long_full_schema):
    columns = ['Field_full_name', 'Field_compact_name', 'Field_name', 'Type', 'Count',
               'Types_count']
    res = ListOutput.mongo_schema_as_dataframe(long_full_schema, columns)
    exp = [['db1', 'coll', 'field', 'field', 'field', 'string', 25359, 'string : 25359'],
           ['db1', 'coll', 'field2', 'field2', 'field2', 'string', 25359, 'string : 25359'],
           ['db1', 'coll', 'field3', 'field3', 'field3', 'ARRAY(OBJECT)', 25359,
            'ARRAY(OBJECT : 25359) : 25359'],
           ['db1', 'coll', 'field3:subfield1', ' : subfield1', 'subfield1', 'string', 25359,
            'string : 25359'],
           ['db1', 'coll', 'field3:subfield2', ' : subfield2', 'subfield2', 'ARRAY(string)',
            25359, 'object : 25359'],
           ['db2', 'coll1', 'field', 'field', 'field', 'string', 25359, 'string : 25359'],
           ['db2', 'coll2', 'field', 'field', 'field', 'string', 25359, 'string : 25359']]
    exp = pd.DataFrame(exp, columns=['Database', 'Collection'] + columns)
    assert_frame_equal(res, exp)


# INTEGRATION LIKE TESTS

def test01_write_txt(schema_ex_df):
    output = os.path.join(TEST_DIR, 'output_data_dict.txt')
    expected_file = os.path.join(TEST_DIR, 'resources', 'expected', 'data_dict.txt')
    output_maker = TxtOutput({})
    output_maker.mongo_schema_df = schema_ex_df.copy()
    with open(output, 'w') as out_fd:
        output_maker.write_output_data(out_fd)
    assert filecmp.cmp(output, expected_file)
    os.remove(output)
    

def test02_write_md(schema_ex_df):
    output = os.path.join(TEST_DIR, 'output_data_dict.md')
    expected_file = os.path.join(TEST_DIR, 'resources', 'expected', 'data_dict.md')
    output_maker = MdOutput({})
    output_maker.mongo_schema_df = schema_ex_df.copy()
    with open(output, 'w') as out_fd:
        output_maker.write_output_data(out_fd)
    assert filecmp.cmp(output, expected_file)
    os.remove(output)
    

def test03_write_html(schema_ex_df):
    output = os.path.join(TEST_DIR, 'output_data_dict.html')
    expected_file = os.path.join(TEST_DIR, 'resources', 'expected', 'data_dict.html')
    output_maker = HtmlOutput({})
    output_maker.mongo_schema_df = schema_ex_df.copy()
    with open(output, 'w') as out_fd:
        output_maker.write_output_data(out_fd)
    with open(output) as out_fd, open(expected_file) as exp_fd:
        assert out_fd.read().replace(' ', '') == exp_fd.read().replace(' ', '')
    os.remove(output)


def test04_write_xlsx(schema_ex_df):
    output = os.path.join(TEST_DIR, 'output_data_dict.xlsx')
    expected_file = os.path.join(TEST_DIR, 'resources', 'expected', 'data_dict.xlsx')
    output_maker = XlsxOutput({})
    output_maker.mongo_schema_df = schema_ex_df.copy()
    output_maker.write_output_data(output)
    res = [cell.value for row in load_workbook(output).active for cell in row]
    exp = [cell.value for row in load_workbook(expected_file).active for cell in row]
    assert res == exp
    os.remove(output)
    

def test05_write_output_dict_schema_txt(schema_ex_dict, columns):
    output = os.path.join(TEST_DIR, 'output_data_dict_from_schema.txt')
    expected_file = os.path.join(TEST_DIR, 'resources', 'expected', 'data_dict.txt')
    arg = {'--format': ['txt', 'txt'], '--output': output,
           '--columns': " ".join(columns)}
    write_output_dict(schema_ex_dict, arg)
    assert filecmp.cmp(output, expected_file)
    os.remove(output)
    

def test06_write_output_dict_schema_md(schema_ex_dict, columns):
    output = os.path.join(TEST_DIR, 'output_data_dict_from_schema.md')
    expected_file = os.path.join(TEST_DIR, 'resources', 'expected', 'data_dict.md')
    arg = {'--format': ['md'], '--output': output,
           '--columns': " ".join(columns)}
    write_output_dict(schema_ex_dict, arg)
    assert filecmp.cmp(output, expected_file)
    os.remove(output)
    

def test07_write_output_dict_schema_html(schema_ex_dict, columns):
    output = os.path.join(TEST_DIR, 'output_data_dict_from_schema.html')
    expected_file = os.path.join(TEST_DIR, 'resources', 'expected', 'data_dict.html')
    arg = {'--format': ['html'], '--output': output,
           '--columns': " ".join(columns)}
    write_output_dict(schema_ex_dict, arg)
    with open(output) as out_fd, open(expected_file) as exp_fd:
        assert out_fd.read().replace(' ', '') == exp_fd.read().replace(' ', '')
    os.remove(output)


def test08_write_output_dict_schema_xlsx(schema_ex_dict, columns):
    output = os.path.join(TEST_DIR, 'output_data_dict_from_schema.xlsx')
    expected_file = os.path.join(TEST_DIR, 'resources', 'expected', 'data_dict.xlsx')
    arg = {'--format': ['xlsx'], '--output': output,
           '--columns': " ".join(columns)}
    write_output_dict(schema_ex_dict, arg)
    res = [cell.value for row in load_workbook(output).active for cell in row]
    exp = [cell.value for row in load_workbook(expected_file).active for cell in row]
    assert res == exp
    os.remove(output)


def test09_write_output_dict_schema_json_without_count(schema_ex_dict):
    output = os.path.join(TEST_DIR, 'output_data_dict_from_schema.json')
    expected_file = os.path.join(TEST_DIR, 'resources', 'expected',
                                 'data_dict_without_counts.json')
    arg = {'--format': ['json'], '--output': output,
           '--without-counts': True}
    write_output_dict(schema_ex_dict, arg)
    with open(output) as out_fd, open(expected_file) as exp_fd:
        assert json.load(out_fd) == json.load(exp_fd)
    os.remove(output)


def test10_write_output_dict_mapping_yaml(mapping_ex_dict):
    output = os.path.join(TEST_DIR, 'output_data_dict_from_mapping.yaml')
    expected_file = os.path.join(TEST_DIR, 'resources', 'expected', 'mapping.yaml')
    arg = {'--format': ['yaml'], '--output': output,
           '--columns': None, '--without-counts': True}
    write_output_dict(mapping_ex_dict, arg)
    assert filecmp.cmp(output, expected_file)
    os.remove(output)


def test11_write_output_dict_wrong_format(mapping_ex_dict):
    arg = {'--format': ['fake'], '--output': None,
           '--columns': None, '--without-counts': True}
    with pytest.raises(ValueError):
        write_output_dict(mapping_ex_dict, arg)


def test12_write_output_dict_schema_non_ascii(columns):
    base_output = "output_fctl_data_dict"
    outputs = {}
    # WARNING: xlsx not actually tested
    extensions = ['txt', 'json', 'html', 'csv', 'xlsx', 'md']
    for ext in extensions:
        outputs[ext] = "{}.{}".format(base_output, ext)
    input_file = os.path.join(TEST_DIR, 'resources', 'input',
                              'test_schema_fr.json')
    arg = {'--format': extensions, '--output': base_output, '--columns': " ".join(columns),
           '--without-counts': False}
    with open(input_file) as f:
        schema_fr = json.loads(f.read())
    write_output_dict(schema_fr, arg)
    extensions.remove('xlsx')
    for ext in extensions:
        with open(outputs[ext]) as f:
            assert 'ÀàÂâÇçÈèÉéÊêËëÎîÏïÔôŒœÙùÛûÜü' in f.read()
    for output in outputs.values():
        os.remove(output)
