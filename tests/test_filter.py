import pytest

from pymongo_schema.filter import *


@pytest.fixture(scope='module')
def schema():
    return {
        "count": 25359,
        "object": {"field": {"types_count": {"string": 25359},
                             "count": 25359, "type": "string",
                             "prop_in_object": 1.0},
                   "field2": {"types_count": {"string": 25359},
                              "count": 25359, "type": "string",
                              "prop_in_object": 1.0},
                   "field3": {"types_count": {"ARRAY": 25359},
                              'array_types_count': {'OBJECT': 25359}, 'array_type': 'OBJECT',
                              "count": 25359, "type": "ARRAY", "prop_in_object": 1.0,
                              "object": {"subfield1": {"types_count": {"string": 25359},
                                                       "count": 25359, "type": "string",
                                                       "prop_in_object": 1.0},
                                         "subfield2": {"types_count": {"object": 25359},
                                                       "count": 25359, "type": "ARRAY",
                                                       'array_type': 'string',
                                                       "prop_in_object": 1.0,
                                                       "array_types_count": {"string": 93463,
                                                                             "null": 738}}}}}}


def test00_init_filtered_schema_simple():
    assert init_filtered_schema({"db.coll": 0}) == {"db": {}}


def test01_init_filtered_schema_empty():
    assert init_filtered_schema({}) == {}


def test02_init_filtered_schema_long():
    namespaces_dict = {"db1.coll1": 0, "db1.coll2": 1, "db2.coll1": []}
    assert init_filtered_schema(namespaces_dict) == {"db1": {}, "db2": {}}


def test03_add_field_to_dict_simple():
    fields_dict = {}
    add_field_to_dict("field", fields_dict)
    assert fields_dict == {"field": "present"}


def test04_add_field_to_dict_long():
    fields_dict = {"field": {"subfield0": "present"}}
    add_field_to_dict("field.subfield.subsubfield", fields_dict)
    assert fields_dict == {"field": {"subfield0": "present",
                                             "subfield": {"subsubfield": "present"}}}


def test05_field_list_to_dict_simple():
    assert field_list_to_dict(["field"]) == {"field": "present"}


def test06_field_list_to_dict_empty():
    assert field_list_to_dict([]) == {}


def test07_field_list_to_dict_long():
    field_list = ["field", "field1.subfield1", "field1.subfield2", "field2.subfield.subsubfield"]
    exp = {"field": "present", "field1": {"subfield1": "present",
                                          "subfield2": "present"},
           "field2": {"subfield": {"subsubfield": "present"}}}
    assert field_list_to_dict(field_list) == exp


def test08_include_fields_from_object_schema_simple(schema):
    expected = {"count": 25359,
                "object": {"field": {"types_count": {"string": 25359}, "count": 25359,
                                     "type": "string", "prop_in_object": 1.0}}}
    assert include_fields_from_object_schema({"field": "present"}, schema) == expected


def test09_include_fields_from_object_schema_empty(schema):
    expected = {"count": 25359,
                "object": {}}
    assert include_fields_from_object_schema({}, schema) == expected


def test10_include_fields_from_object_schema_all(schema):
    fields = {"field": "present", "field2": "present", "field3": "present"}
    assert include_fields_from_object_schema(fields, schema) == schema


def test11_include_fields_from_object_schema_long(schema):
    fields = {"field3": {"subfield1": "present"}}
    expected = {"count": 25359, "object": {"field3": {
        "types_count": {"ARRAY": 25359},
        "count": 25359, "type": "ARRAY", 'array_types_count': {'OBJECT': 25359},
        'array_type': 'OBJECT',"prop_in_object": 1.0,
        "object": {"subfield1": schema["object"]["field3"]["object"]["subfield1"]}}}}
    assert include_fields_from_object_schema(fields, schema) == expected


def test12_exclude_fields_from_object_schema_simple(schema):
    schema = deepcopy(schema)
    expected = deepcopy(schema)
    expected["object"].pop("field")
    exclude_fields_from_object_schema({"field": "present"}, schema["object"])
    assert schema == expected

def test13_exclude_fields_from_object_schema_empty(schema):
    schema = deepcopy(schema)
    exclude_fields_from_object_schema(
        {"field": "present", "field2": "present", "field3": "present"}, schema["object"])
    assert schema["object"] == {}


def test14_exclude_fields_from_object_schema_all(schema):
    schema = deepcopy(schema)
    expected = deepcopy(schema)
    exclude_fields_from_object_schema({}, schema)
    assert schema == expected


def test15_exclude_fields_from_object_schema_long(schema):
    schema = deepcopy(schema)
    fields = {"field3": {"subfield1": "present"}}
    expected = deepcopy(schema)
    expected["object"]["field3"]["object"].pop("subfield1")
    exclude_fields_from_object_schema(fields, schema["object"])
    assert schema == expected


def test16_filter_mongo_schema_namespaces(schema):    # functional test
    schema = {"db1": {"coll1": deepcopy(schema),
                      "coll2": deepcopy(schema)},
              "db2": {"coll1": deepcopy(schema),
                      "coll2": deepcopy(schema)}}
    namespaces = {"db1.coll1": True,
                  "db1.coll2": {"includeFields": ["field", "field3.subfield1"]},
                  "db2.coll2": {"excludeFields": ["field", "field3.subfield1"]},
                  "db2.coll1": False}
    expected = deepcopy(schema)
    expected["db1"]["coll2"]["object"].pop("field2")
    expected["db1"]["coll2"]["object"]["field3"]["object"].pop("subfield2")
    expected["db2"]["coll2"]["object"].pop("field")
    expected["db2"]["coll2"]["object"]["field3"]["object"].pop("subfield1")
    expected["db2"].pop("coll1")
    assert filter_mongo_schema_namespaces(schema, namespaces) == expected
