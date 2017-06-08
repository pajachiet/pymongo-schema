import unittest

from pymongo_schema import extract
from pymongo import MongoClient
import json
from pprint import pprint


class TestExport(unittest.TestCase):
    def test_add_value_type(self):
        field_schema = {
            'types_count': {
                "string": 3
            }
        }

        extract.add_value_type("test", field_schema, 'types_count')
        self.assertEqual(field_schema['types_count']['string'], 4)

    def test_extract_pymongo_client_schema(self):
        """
        Import the expected db schema and compare it with the schema extracted from the mongodb test database.
        :return: 
        """

        pymongo_client = MongoClient('localhost', 27017)

        with open('tests/test_schema.json') as data_file:
            mongo_schema_expected = json.load(data_file, encoding='utf-8')

        mongo_schema_got = extract.extract_pymongo_client_schema(pymongo_client,
                                                                 database_names='test_db',
                                                                 collection_names='test_col')

        self.assertEqual(mongo_schema_got, mongo_schema_expected)


if __name__ == '__main__':
    unittest.main()
