import unittest

from pymongo_schema.mongo_sql_types import *


class TestMongoSqlTypes(unittest.TestCase):
    def test00_get_type_string(self):
        self.assertEqual(get_type_string([]), 'ARRAY')
        self.assertEqual(get_type_string({}), 'OBJECT')
        self.assertEqual(get_type_string({'a': []}), 'OBJECT')
        self.assertEqual(get_type_string(None), 'null')
        self.assertEqual(get_type_string(1.5), 'float')
        self.assertEqual(get_type_string(set()), 'unknown')
        self.assertEqual(get_type_string(unittest.TestCase), 'unknown')

    def test01_common_parent_type(self):
        self.assertEqual(common_parent_type([]), 'null')
        self.assertEqual(common_parent_type(['string']), 'string')
        self.assertEqual(common_parent_type(['integer', 'boolean']), 'integer')
        self.assertEqual(common_parent_type(['integer', 'integer']), 'integer')
        self.assertEqual(common_parent_type(['integer', 'float']), 'number')
        self.assertEqual(common_parent_type(['integer', 'unknown']), 'general_scalar')
        self.assertEqual(common_parent_type(['integer', 'OBJECT']), 'mixed_scalar_object')


if __name__ == '__main__':
    unittest.main()
