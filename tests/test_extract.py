import json
import os
import unittest

from pymongo import MongoClient

from pymongo_schema.extract import *

TEST_DIR = os.path.dirname(__file__)


class TestExtract(unittest.TestCase):
    def test00_default_to_regular_dict(self):
        d = recursive_default_to_regular_dict(defaultdict(int))
        self.assertRaises(KeyError, lambda: d[1])

    def test01_default_to_regular_dict_rec(self):
        d = recursive_default_to_regular_dict({'a': defaultdict(int)})
        self.assertRaises(KeyError, lambda: d['a'][1])

    def test02_add_value_type(self):
        schema = {'types_count': {'integer': 5}}
        add_value_type(2, schema)
        self.assertEqual(schema, {'types_count': {'integer': 6}})

    def test03_add_list_to_schema_simple(self):
        schema = {}
        add_potential_list_to_field_schema([1, 2, 3], schema)
        self.assertEqual(schema, {'array_types_count': {'integer': 3}})

    def test04_add_list_to_schema_empty(self):
        schema = {}
        add_potential_list_to_field_schema([], schema)
        self.assertEqual(schema, {'array_types_count': {'null': 1}})

    def test05_add_list_to_schema_empty_not_list(self):
        schema = {}
        add_potential_list_to_field_schema(5, schema)
        self.assertEqual(schema, {})

    def test06_add_list_to_schema_empty_long(self):
        schema = {}
        add_potential_list_to_field_schema([{"a": 1}], schema)
        self.assertEqual(schema, {'array_types_count': {"OBJECT": 1},
                                  "object": {"a": {"types_count": {"integer": 1}, "count": 1}}})

    def test07_add_doc_to_schema_simple(self):
        schema = {}
        add_potential_document_to_field_schema({"a": 1}, schema)
        self.assertEqual(schema, {"object": {"a": {"types_count": {"integer": 1}, "count": 1}}})

    def test08_add_doc_to_schema_empty(self):
        schema = {}
        add_potential_document_to_field_schema({}, schema)
        self.assertEqual(schema, {"object": {}})

    def test09_add_doc_to_schema_not_dict(self):
        schema = {}
        add_potential_document_to_field_schema(5, schema)
        self.assertEqual(schema, {})

    def test10_add_doc_to_object_schema(self):
        schema = init_empty_object_schema()
        add_document_to_object_schema({"a": 1, "b": [1, 2, {}], "c": {"a": []}}, schema)
        expected = {'a': {'types_count': {'integer': 1}, 'count': 1},
                    'c': {'types_count': {'OBJECT': 1}, 'count': 1,
                          'object': {'a': {'types_count': {'ARRAY': 1}, 'count': 1,
                                           'array_types_count': {'null': 1}}}},
                    'b': {'types_count': {'ARRAY': 1}, 'count': 1, 'object': {},
                          'array_types_count': {'integer': 2, 'OBJECT': 1}}}
        self.assertEqual(schema, expected)

    def test11_summarize_types_simple(self):
        schema = {'types_count': {'integer': 1}, 'count': 1}
        summarize_types(schema)
        self.assertEqual(schema, {'types_count': {'integer': 1}, 'count': 1, 'type': 'integer'})

    def test12_summarize_types_long(self):
        schema = {'types_count': {'ARRAY': 1}, 'count': 1, 'object': {},
                  'array_types_count': {'integer': 2, "string": 1, 'OBJECT': 1}}
        expected = {'types_count': {'ARRAY': 1}, 'count': 1,
                    'array_types_count': {'integer': 2, 'OBJECT': 1, 'string': 1},
                    'object': {},
                    'array_type': 'mixed_scalar_object', 'type': 'ARRAY'}
        summarize_types(schema)
        self.assertEqual(schema, expected)


class TestExtractUsingMongo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pymongo_client = MongoClient()

    def test_extract_schema(self):
        with open(os.path.join(TEST_DIR, 'test_schema.json')) as data_file:
            mongo_schema_expected = json.load(data_file, encoding='utf-8')

        mongo_schema_got = extract_pymongo_client_schema(self.pymongo_client,
                                                         database_names='test_db',
                                                         collection_names='test_col')

        self.assertEqual(mongo_schema_got, mongo_schema_expected)

if __name__ == '__main__':
    unittest.main()