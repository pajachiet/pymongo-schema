import unittest

from pymongo_schema.tosql import *


class TestTosql(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.simple_schema = {'count': 25359,
                             'object': {'_id': {"types_count": {"oid": 25359}, "count": 25359,
                                                "type": "oid", "prop_in_object": 1.0},
                                        'field': {'types_count': {'string': 25359},
                                                  'count': 25359, 'type': 'string',
                                                  'prop_in_object': 1.0}}}
        cls.long_schema = {
            'count': 25359,
            'object': {'_id': {"types_count": {"oid": 25359}, "count": 25359,
                               "type": "oid", "prop_in_object": 1.0},
                       'field': {'types_count': {'string': 25359}, 'count': 25359, 'type': 'string',
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

    def test00_init_collection_mapping_simple(self):
        mapping = {}
        res = init_collection_mapping('coll', mapping, self.simple_schema)
        self.assertTrue(res)
        self.assertEqual(mapping, {'coll': {'pk': '_id', '_id': {'type': 'TEXT'}}})

    def test01_init_collection_mapping_long(self):
        mapping = {'coll0': {}}
        res = init_collection_mapping('coll', mapping, self.long_schema)
        self.assertTrue(res)
        self.assertEqual(mapping, {'coll0': {}, 'coll': {'pk': '_id', '_id': {'type': 'TEXT'}}})

    def test02_add_field_to_table_mapping_simple(self):
        mapping = {}
        add_field_to_table_mapping('field', mapping, 'integer')
        self.assertEqual(mapping, {'field': {'dest': 'field', 'type': 'INT'}})

    def test03_add_field_to_table_mapping_long(self):
        mapping = {'field0': {}}
        add_field_to_table_mapping('field1.subfield', mapping, 'string')
        self.assertEqual(mapping, {'field0': {},
                                   'field1.subfield': {'dest': 'field1__subfield', 'type': 'TEXT'}})

    def test04_initiate_array_mapping(self):
        mapping = {'parent_table': {'pk': '_id', '_id': {'type': 'TEXT'}}}
        res = initiate_array_mapping('array_field', mapping, 'parent_table')
        exp = {'parent_table':
                   {'pk': '_id', '_id': {'type': 'TEXT'},
                    'array_field': {'dest': 'parent_table__array_field',
                                    'fk': 'id_parent_table'}},
               'parent_table__array_field': {'id_parent_table': {'type': 'TEXT'},
                                             'pk': 'id'}}
        self.assertEqual(res, 'parent_table__array_field')
        self.assertEqual(mapping, exp)

    def test05_add_scalar_array(self):
        mapping = {'parent_table': {'pk': '_id', '_id': {'type': 'TEXT'}}}
        add_scalar_array_field_to_mapping('scalar_array_field', 'scalar_array_field',
                                          'boolean', mapping, 'parent_table')

        exp = {'parent_table': {'pk': '_id', '_id': {'type': 'TEXT'},
                                'scalar_array_field': {'dest': 'parent_table__scalar_array_field',
                                                       'fk': 'id_parent_table',
                                                       'type': '_ARRAY_OF_SCALARS',
                                                       'valueField': 'scalar_array_field'}},
               'parent_table__scalar_array_field': {'id_parent_table': {'type': 'TEXT'},
                                                    'pk': 'id',
                                                    'scalar_array_field':
                                                        {'dest': 'scalar_array_field',
                                                         'type': 'BOOLEAN'}}}
        self.assertEqual(mapping, exp)

    def test06_mongo_schema_to_mapping_simple(self):
        res = mongo_schema_to_mapping({'db': {'coll': self.simple_schema}})
        self.assertEqual(res, {'db': {'coll': {'pk': '_id',
                                               'field': {'dest': 'field', 'type': 'TEXT'},
                                               '_id': {'dest': '_id', 'type': 'TEXT'}}}})

    def test07_mongo_schema_to_mapping_empty(self):
        res = mongo_schema_to_mapping({'db': {'coll': {'count': 0, 'object': {}}}})
        self.assertEqual(res, {'db': {}})

    def test08_mongo_schema_to_mapping_wrong_type_field(self):
        wrong_schema = {'count': 25359,
                        'object': {'_id': {"types_count": {"oid": 25359}, "count": 25359,
                                           "type": "oid", "prop_in_object": 1.0},
                                   'field1': {'types_count': {'unknown': 25359}, 'count': 25359,
                                              'type': 'unknown', 'prop_in_object': 1.0},
                                   'field2': {'types_count': {'string': 25359}, 'count': 25359,
                                              'type': 'string', 'prop_in_object': 1.0}}}
        res = mongo_schema_to_mapping({'db': {'coll': wrong_schema}})
        exp = {'db': {'coll': {'field2': {'dest': 'field2', 'type': 'TEXT'},
                               'pk': '_id', '_id': {'dest': '_id', 'type': 'TEXT'}}}}
        self.assertEqual(res, exp)

    def test09_mongo_schema_to_mapping_wrong_type_id(self):
        wrong_schema = {'count': 25359,
                        'object': {'_id': {"types_count": {"unknown": 25359}, "count": 25359,
                                           "type": "unknown", "prop_in_object": 1.0},
                                   'field1': {'types_count': {'unknown': 25359}, 'count': 25359,
                                              'type': 'unknown', 'prop_in_object': 1.0},
                                   'field2': {'types_count': {'string': 25359}, 'count': 25359,
                                              'type': 'string', 'prop_in_object': 1.0}}}
        res = mongo_schema_to_mapping({'db': {'coll': wrong_schema}})
        self.assertEqual(res, {'db': {}})

    def test10_mongo_schema_to_mapping_long(self):
        res = mongo_schema_to_mapping({'db1': {'coll1': self.long_schema},
                                       'db2': {'coll1': self.simple_schema,
                                               'coll2': self.simple_schema}})
        exp = {'db1':
                   {'coll1__field3__subfield2':
                        {'id_coll1__field3': {'type': 'TEXT'},
                         'pk': 'id',
                         'subfield2': {'dest': 'subfield2', 'type': 'TEXT'}},
                    'coll1': {'field2': {'dest': 'field2', 'type': 'TEXT'},
                              'pk': '_id',
                              '_id': {'dest': '_id', 'type': 'TEXT'},
                              'field3': {'dest': 'coll1__field3',
                                         'fk': 'id_coll1',
                                         'type': '_ARRAY'},
                              'field': {'dest': 'field', 'type': 'TEXT'}},
                    'coll1__field3': {'pk': 'id',
                                      'subfield2': {'dest': 'coll1__field3__subfield2',
                                                    'fk': 'id_coll1__field3',
                                                    'type': '_ARRAY_OF_SCALARS',
                                                    'valueField': 'subfield2'},
                                      'subfield1': {'dest': 'subfield1', 'type': 'TEXT'},
                                      'id_coll1': {'type': 'TEXT'}}},
               'db2': {'coll2': {'pk': '_id', '_id': {'dest': '_id', 'type': 'TEXT'},
                                 'field': {'dest': 'field', 'type': 'TEXT'}},
                       'coll1': {'pk': '_id', '_id': {'dest': '_id', 'type': 'TEXT'},
                                 'field': {'dest': 'field', 'type': 'TEXT'}}}}
        self.assertEqual(res, exp)


if __name__ == '__main__':
    unittest.main()
