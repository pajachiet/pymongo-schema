import pytest

from pymongo_schema.mongo_sql_types import *


def test00_get_type_string():
    assert get_type_string([]) == 'ARRAY'
    assert get_type_string({}) == 'OBJECT'
    assert get_type_string({'a': []}) == 'OBJECT'
    assert get_type_string(None) == 'null'
    assert get_type_string(1.5) == 'float'
    assert get_type_string(set()) == 'unknown'
    assert get_type_string(pytest.File) == 'unknown'


def test01_common_parent_type():
    assert common_parent_type([]) == 'null'
    assert common_parent_type(['string']) == 'string'
    assert common_parent_type(['integer', 'boolean']) == 'integer'
    assert common_parent_type(['integer', 'integer']) == 'integer'
    assert common_parent_type(['integer', 'float']) == 'number'
    assert common_parent_type(['integer', 'unknown']) == 'general_scalar'
    assert common_parent_type(['integer', 'OBJECT']) == 'mixed_scalar_object'
