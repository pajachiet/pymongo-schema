import unittest

from pymongo_schema import extract


class TestExport(unittest.TestCase):
    def test_add_value_type(self):
        field_schema = {
            'types_count': {
                "string": 3
            }
        }

        extract.add_value_type("test", field_schema, 'types_count')
        self.assertEqual(field_schema['types_count']['string'], 4)


if __name__ == '__main__':
    unittest.main()
