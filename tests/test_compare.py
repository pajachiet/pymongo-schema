import os

import pytest

from pymongo_schema.compare import *

TEST_DIR = os.path.dirname(__file__)


@pytest.fixture(scope='module')
def df_columns():
    return ['Database', 'Collection', 'Hierarchy', 'In Schema', 'In Expected']


@pytest.fixture(scope='module')
def long_diff():
    return [{'hierarchy': '', 'prev_schema': 'db0', 'new_schema': None},
            {'hierarchy': '', 'prev_schema': None, 'new_schema': 'db1'},
            {'hierarchy': 'db', 'prev_schema': 'coll1', 'new_schema': None},
            {'hierarchy': 'db', 'prev_schema': None, 'new_schema': 'coll2'},
            {'hierarchy': 'db.coll', 'prev_schema': 'field2', 'new_schema': None},
            {'hierarchy': 'db.coll', 'prev_schema': None, 'new_schema': 'field4'},
            {'hierarchy': 'db.coll.field3', 'prev_schema': {'type': 'boolean'},
             'new_schema': {'type': 'string'}},
            {'hierarchy': 'db.coll.field.array_subfield',
             'prev_schema': None, 'new_schema': 'subsubfield2'},
            {'hierarchy': 'db.coll.field.array_subfield.subsubfield',
             'prev_schema': {'type': 'integer'}, 'new_schema': {'type': 'boolean'}},
            {'hierarchy': 'db.coll.field5', 'prev_schema': {'array_type': 'string'},
             'new_schema': {'array_type': 'integer'}},
            {'hierarchy': 'db.coll.field6', 'prev_schema': {'type': 'ARRAY'},
             'new_schema': {'type': 'string'}}]


def test00_compare_schemas_bases_very_simple():
    schema = {'db': {'coll': {'object': {'field1': {'type': 'string'}}}}}
    assert compare_schemas_bases(schema, schema) == []


def test01_compare_schemas_bases_diff_keys():
    prev_schema = {'db': {'coll': {'object': {
        'field1': {'type': 'string', 'description': 'a_description'}}}}}
    new_schema = {'db': {'coll': {'object': {
        'field1': {'type': 'string', 'types_count': {'string': 1}}}}}}
    assert compare_schemas_bases(prev_schema, new_schema) == []


def test02_compare_schemas_bases_simple_diff():
    prev_schema = {'db': {'coll': {'object': {'field1': {'type': 'string'},
                                              'field2': {'type': 'integer'},
                                              'field3': {'type': 'boolean'},
                                              'field5': {'type': 'ARRAY', 'array_type': 'string'},
                                              'field6': {'type': 'ARRAY', 'array_type': 'string'}}},
                          'coll1': {}},
                   'db0': {}}
    new_schema = {'db': {'coll': {'object': {'field1': {'type': 'string', 'count': 1},
                                             'field3': {'type': 'string'},
                                             'field4': {'type': 'boolean'},
                                             'field5': {'type': 'ARRAY', 'array_type': 'integer'},
                                             'field6': {'type': 'string'}
                                             }},
                         'coll2': {}},
                  'db1': {}}
    exp_diff = [{'hierarchy': '', 'prev_schema': 'db0', 'new_schema': None},
                {'hierarchy': '', 'prev_schema': None, 'new_schema': 'db1'},
                {'hierarchy': 'db', 'prev_schema': 'coll1', 'new_schema': None},
                {'hierarchy': 'db', 'prev_schema': None, 'new_schema': 'coll2'},
                {'hierarchy': 'db.coll', 'prev_schema': 'field2', 'new_schema': None},
                {'hierarchy': 'db.coll', 'prev_schema': None, 'new_schema': 'field4'},
                {'hierarchy': 'db.coll.field3', 'prev_schema': {'type': 'boolean'},
                 'new_schema': {'type': 'string'}},
                {'hierarchy': 'db.coll.field5', 'prev_schema': {'array_type': 'string'},
                 'new_schema': {'array_type': 'integer'}},
                {'hierarchy': 'db.coll.field6', 'prev_schema': {'type': 'ARRAY'},
                 'new_schema': {'type': 'string'}}]
    res = compare_schemas_bases(prev_schema, new_schema)
    assert res == exp_diff


def test03_compare_schema_nested():
    prev_schema = {'db': {'coll': {'object': {
        'field': {'type': 'ARRAY', 'array_type': 'OBJECT', 'object': {
            'array_subfield': {'type': 'OBJECT', 'object': {
                'subsubfield': {'type': 'integer'}
            }}}}}}}}
    new_schema = {'db': {'coll': {'object': {
        'field': {'type': 'ARRAY', 'array_type': 'OBJECT', 'object': {
            'array_subfield': {'type': 'OBJECT', 'object': {
                'subsubfield': {'type': 'boolean'},
                'subsubfield2': {'type': 'boolean'}
            }}}}}}}}
    exp_diff = [{'hierarchy': 'db.coll.field.array_subfield',
                 'prev_schema': None, 'new_schema': 'subsubfield2'},
                {'hierarchy': 'db.coll.field.array_subfield.subsubfield',
                 'prev_schema': {'type': 'integer'}, 'new_schema': {'type': 'boolean'}}]
    res = compare_schemas_bases(prev_schema, new_schema)
    assert res == exp_diff


def test04_is_retrocompatible_true():
    diff = [{'hierarchy': '', 'prev_schema': 'db0', 'new_schema': None},
            {'hierarchy': '', 'prev_schema': None, 'new_schema': 'db1'},
            {'hierarchy': 'db', 'prev_schema': 'coll1', 'new_schema': None},
            {'hierarchy': 'db', 'prev_schema': None, 'new_schema': 'coll2'},
            {'hierarchy': 'db.coll', 'prev_schema': 'field2', 'new_schema': None},
            {'hierarchy': 'db.coll', 'prev_schema': None, 'new_schema': 'field4'},
            {'hierarchy': 'db.coll.field', 'prev_schema': None, 'new_schema': 'subfield'}]
    assert is_retrocompatible(diff)


def test05_is_retrocompatible_false(long_diff):
    assert not is_retrocompatible(long_diff)
