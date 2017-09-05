import json
import os

import pytest
from pymongo import MongoClient

from pymongo_schema.extract import *

TEST_DIR = os.path.dirname(__file__)


@pytest.yield_fixture(scope='module')
def pymongo_client():
    conn = MongoClient()
    try:
        yield conn
    finally:
        conn.close()


def test00_default_to_regular_dict():
    d = recursive_default_to_regular_dict(defaultdict(int))
    with pytest.raises(KeyError):
        d[1]


def test01_default_to_regular_dict_rec():
    d = recursive_default_to_regular_dict({'a': defaultdict(int)})
    with pytest.raises(KeyError):
        d['a'][1]


def test02_add_value_type():
    schema = {'types_count': {'integer': 5}}
    add_value_type(2, schema)
    assert schema == {'types_count': {'integer': 6}}


def test03_add_list_to_schema_simple():
    schema = {}
    add_potential_list_to_field_schema([1, 2, 3], schema)
    assert schema == {'array_types_count': {'integer': 3}}


def test04_add_list_to_schema_empty():
    schema = {}
    add_potential_list_to_field_schema([], schema)
    assert schema == {'array_types_count': {'null': 1}}


def test05_add_list_to_schema_empty_not_list():
    schema = {}
    add_potential_list_to_field_schema(5, schema)
    assert schema == {}


def test06_add_list_to_schema_empty_long():
    schema = {}
    add_potential_list_to_field_schema([{"a": 1}], schema)
    assert schema == {'array_types_count': {"OBJECT": 1},
                      "object": {"a": {"types_count": {"integer": 1}, "count": 1}}}


def test07_add_doc_to_schema_simple():
    schema = {}
    add_potential_document_to_field_schema({"a": 1}, schema)
    assert schema == {"object": {"a": {"types_count": {"integer": 1}, "count": 1}}}


def test08_add_doc_to_schema_empty():
    schema = {}
    add_potential_document_to_field_schema({}, schema)
    assert schema == {"object": {}}


def test09_add_doc_to_schema_not_dict():
    schema = {}
    add_potential_document_to_field_schema(5, schema)
    assert schema == {}


def test10_add_doc_to_object_schema():
    schema = init_empty_object_schema()
    add_document_to_object_schema({"a": 1, "b": [1, 2, {}], "c": {"a": []}}, schema)
    expected = {'a': {'types_count': {'integer': 1}, 'count': 1},
                'c': {'types_count': {'OBJECT': 1}, 'count': 1,
                      'object': {'a': {'types_count': {'ARRAY': 1}, 'count': 1,
                                       'array_types_count': {'null': 1}}}},
                'b': {'types_count': {'ARRAY': 1}, 'count': 1, 'object': {},
                      'array_types_count': {'integer': 2, 'OBJECT': 1}}}
    assert schema == expected


def test11_summarize_types_simple():
    schema = {'types_count': {'integer': 1}, 'count': 1}
    summarize_types(schema)
    assert schema == {'types_count': {'integer': 1}, 'count': 1, 'type': 'integer'}


def test12_summarize_types_long():
    schema = {'types_count': {'ARRAY': 1}, 'count': 1, 'object': {},
              'array_types_count': {'integer': 2, "string": 1, 'OBJECT': 1}}
    expected = {'types_count': {'ARRAY': 1}, 'count': 1,
                'array_types_count': {'integer': 2, 'OBJECT': 1, 'string': 1},
                'object': {},
                'array_type': 'mixed_scalar_object', 'type': 'ARRAY'}
    summarize_types(schema)
    assert schema == expected


def test_extract_schema(pymongo_client):
    with open(os.path.join(TEST_DIR, 'resources', 'expected', 'schema.json')) as data_file:
        mongo_schema_expected = json.load(data_file, encoding='utf-8')

    mongo_schema_got = extract_pymongo_client_schema(pymongo_client,
                                                     database_names='test_db',
                                                     collection_names='test_col')

    assert mongo_schema_got == mongo_schema_expected
