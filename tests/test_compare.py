import os

import pytest

from pymongo_schema.compare import *

TEST_DIR = os.path.dirname(__file__)


@pytest.fixture(scope='module')
def df_columns():
    return ['Database', 'Collection', 'Hierarchy', 'In Schema', 'In Expected']


@pytest.fixture(scope='module')
def long_diff():
    return [{'hierarchy': '', 'schema': 'db0', 'expected': None},
            {'hierarchy': '', 'schema': None, 'expected': 'db1'},
            {'hierarchy': 'db', 'schema': 'coll1', 'expected': None},
            {'hierarchy': 'db', 'schema': None, 'expected': 'coll2'},
            {'hierarchy': 'db.coll', 'schema': 'field2', 'expected': None},
            {'hierarchy': 'db.coll', 'schema': None, 'expected': 'field4'},
            {'hierarchy': 'db.coll.field3', 'schema': {'type': 'boolean'},
             'expected': {'type': 'string'}},
            {'hierarchy': 'db.coll.field.array_subfield',
             'schema': None, 'expected': 'subsubfield2'},
            {'hierarchy': 'db.coll.field.array_subfield.subsubfield',
             'schema': {'type': 'integer'}, 'expected': {'type': 'boolean'}},
            {'hierarchy': 'db.coll.field5', 'schema': {'array_type': 'string'},
             'expected': {'array_type': 'integer'}},
            {'hierarchy': 'db.coll.field6', 'schema': {'type': 'ARRAY'},
             'expected': {'type': 'string'}}]


def test00_compare_schemas_bases_very_simple():
    schema = {'db': {'coll': {'object': {'field1': {'type': 'string'}}}}}
    assert compare_schemas_bases(schema, schema) == []


def test01_compare_schemas_bases_diff_keys():
    schema = {'db': {'coll': {'object': {
        'field1': {'type': 'string', 'description': 'a_description'}}}}}
    exp_schema = {'db': {'coll': {'object': {
        'field1': {'type': 'string', 'types_count': {'string': 1}}}}}}
    assert compare_schemas_bases(schema, exp_schema) == []


def test02_compare_schemas_bases_simple_diff():
    schema = {'db': {'coll': {'object': {'field1': {'type': 'string'},
                                         'field2': {'type': 'integer'},
                                         'field3': {'type': 'boolean'},
                                         'field5': {'type': 'ARRAY', 'array_type': 'string'},
                                         'field6': {'type': 'ARRAY', 'array_type': 'string'}}},
                     'coll1': {}},
              'db0': {}}
    exp_schema = {'db': {'coll': {'object': {'field1': {'type': 'string', 'count': 1},
                                             'field3': {'type': 'string'},
                                             'field4': {'type': 'boolean'},
                                             'field5': {'type': 'ARRAY', 'array_type': 'integer'},
                                             'field6': {'type': 'string'}
                                             }},
                         'coll2': {}},
                  'db1': {}}
    exp_diff = [{'hierarchy': '', 'schema': 'db0', 'expected': None},
                {'hierarchy': '', 'schema': None, 'expected': 'db1'},
                {'hierarchy': 'db', 'schema': 'coll1', 'expected': None},
                {'hierarchy': 'db', 'schema': None, 'expected': 'coll2'},
                {'hierarchy': 'db.coll', 'schema': 'field2', 'expected': None},
                {'hierarchy': 'db.coll', 'schema': None, 'expected': 'field4'},
                {'hierarchy': 'db.coll.field3', 'schema': {'type': 'boolean'},
                 'expected': {'type': 'string'}},
                {'hierarchy': 'db.coll.field5', 'schema': {'array_type': 'string'},
                 'expected': {'array_type': 'integer'}},
                {'hierarchy': 'db.coll.field6', 'schema': {'type': 'ARRAY'},
                 'expected': {'type': 'string'}}]
    res = compare_schemas_bases(schema, exp_schema)
    assert res == exp_diff


def test03_compare_schema_nested():
    schema = {'db': {'coll': {'object': {
        'field': {'type': 'ARRAY', 'array_type': 'OBJECT', 'object': {
            'array_subfield': {'type': 'OBJECT', 'object': {
                'subsubfield': {'type': 'integer'}
            }}}}}}}}
    exp_schema = {'db': {'coll': {'object': {
        'field': {'type': 'ARRAY', 'array_type': 'OBJECT', 'object': {
            'array_subfield': {'type': 'OBJECT', 'object': {
                'subsubfield': {'type': 'boolean'},
                'subsubfield2': {'type': 'boolean'}
            }}}}}}}}
    exp_diff = [{'hierarchy': 'db.coll.field.array_subfield',
                 'schema': None, 'expected': 'subsubfield2'},
                {'hierarchy': 'db.coll.field.array_subfield.subsubfield',
                 'schema': {'type': 'integer'}, 'expected': {'type': 'boolean'}}]
    res = compare_schemas_bases(schema, exp_schema)
    assert res == exp_diff
