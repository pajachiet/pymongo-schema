import json
import os
import unittest

from pymongo import MongoClient

from pymongo_schema.extract import extract_pymongo_client_schema
from pymongo_schema.tosql import mongo_schema_to_mapping

TEST_DIR = os.path.dirname(__file__)


class TestFunctinonal(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pymongo_client = MongoClient()

    def test_from_mongo_to_mapping(self):
        with open(os.path.join(TEST_DIR, "resources", "input", "mapping.json")) as f:
            exp_mapping = json.load(f)
        mongo_schema = extract_pymongo_client_schema(self.pymongo_client,
                                                     database_names='test_db',
                                                     collection_names='test_col')

        mapping = mongo_schema_to_mapping(mongo_schema)
        self.assertEqual(mapping, exp_mapping)

if __name__ == '__main__':
    unittest.main()