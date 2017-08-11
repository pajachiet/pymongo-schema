import filecmp
import json
import os
import sys
import unittest
from itertools import chain

from openpyxl import load_workbook
from pymongo import MongoClient

from pymongo_schema.extract import extract_pymongo_client_schema
from pymongo_schema.tosql import mongo_schema_to_mapping
from pymongo_schema.command_line import main

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


class TestCommandLine(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = os.path.join(TEST_DIR, 'resources', 'input', 'test_schema.json')

    def setUp(self):
        self.output = None

    def tearDown(self):
        if self.output and not all(sys.exc_info()):
            if isinstance(self.output, str):
                os.remove(self.output)
            else:
                for output in self.output:
                    os.remove(output)

    def test01_extract(self):
        self.output = os.path.join(TEST_DIR, "output_fctl_schema.json")
        expected_file = os.path.join(TEST_DIR, 'resources', 'expected', 'schema.json')
        argv = ['extract', '--database', 'test_db', '--collection', 'test_col',
                '--output', self.output, '--format', 'json']
        main(argv)
        with open(self.output) as out_f, open(expected_file) as exp_f:
            self.assertEqual(json.load(out_f), json.load(exp_f))

    def test02_transform(self):
        base_output = "output_fctl_data_dict"
        outputs = {}
        extensions = ["txt", 'html', 'xlsx', 'csv']
        for ext in extensions:
            outputs[ext] = "{}.{}".format(base_output, ext)
        self.output = outputs.values()

        exp = os.path.join(TEST_DIR, 'resources', 'expected', 'data_dict')
        argv = ['transform', '--input', self.schema, '--output', base_output, '--columns',
                'Field_compact_name Field_name Full_name Description Count Percentage Types_count']
        argv += chain.from_iterable([['--format', fmt] for fmt in extensions])
        main(argv)

        self.assertTrue(filecmp.cmp(outputs['txt'], "{}.txt".format(exp)))
        self.assertTrue(filecmp.cmp(outputs['csv'], "{}.csv".format(exp)))
        with open(outputs['html']) as out_fd, \
                open("{}.html".format(exp)) as exp_fd:
            self.assertEqual(out_fd.read().replace(' ', ''), exp_fd.read().replace(' ', ''))
        res = [cell.value for row in load_workbook(outputs['xlsx']).active for cell in row]
        exp = [cell.value for row in load_workbook("{}.xlsx".format(exp)).active for cell in row]
        self.assertEqual(res, exp)

    def test03_transform_filter(self):
        base_output = "output_fctl_data_dict_filtered"
        outputs = {}
        extensions = ["txt", 'html', 'xlsx', 'csv']
        for ext in extensions:
            outputs[ext] = "{}.{}".format(base_output, ext)
        self.output = outputs.values()

        namespace = os.path.join(TEST_DIR, 'resources', 'input', 'namespace.json')
        exp = os.path.join(TEST_DIR, 'resources', 'expected', 'data_dict_filtered')
        argv = ['transform', '--input', self.schema, '--output', base_output,
                '--filter', namespace, '--columns',
                'Field_compact_name Field_name Full_name Description Count Percentage Types_count']
        argv += chain.from_iterable([['--format', fmt] for fmt in extensions])
        main(argv)

        self.assertTrue(filecmp.cmp(outputs['txt'], "{}.txt".format(exp)))
        self.assertTrue(filecmp.cmp(outputs['csv'], "{}.csv".format(exp)))
        with open(outputs['html']) as out_fd, \
                open("{}.html".format(exp)) as exp_fd:
            self.assertEqual(out_fd.read().replace(' ', ''), exp_fd.read().replace(' ', ''))
        res = [cell.value for row in load_workbook(outputs['xlsx']).active for cell in row]
        exp = [cell.value for row in load_workbook("{}.xlsx".format(exp)).active for cell in row]
        self.assertEqual(res, exp)

    def test04_transform_default_cols(self):
        base_output = "output_fctl_data_dict_default"
        outputs = {}
        extensions = ["txt", 'html', 'xlsx', 'csv']
        for ext in extensions:
            outputs[ext] = "{}.{}".format(base_output, ext)
        self.output = outputs.values()

        exp = os.path.join(TEST_DIR, 'resources', 'expected', 'data_dict_default')
        argv = ['transform', '--input', self.schema, '--output', base_output]
        argv += chain.from_iterable([['--format', fmt] for fmt in extensions])
        main(argv)

        self.assertTrue(filecmp.cmp(outputs['txt'], "{}.txt".format(exp)))
        self.assertTrue(filecmp.cmp(outputs['csv'], "{}.csv".format(exp)))
        with open(outputs['html']) as out_fd, \
                open("{}.html".format(exp)) as exp_fd:
            self.assertEqual(out_fd.read().replace(' ', ''), exp_fd.read().replace(' ', ''))
        res = [cell.value for row in load_workbook(outputs['xlsx']).active for cell in row]
        exp = [cell.value for row in load_workbook("{}.xlsx".format(exp)).active for cell in row]
        self.assertEqual(res, exp)

    def test05_tosql(self):
        self.output = os.path.join(TEST_DIR, "output_fctl_mapping.json")
        exp = os.path.join(TEST_DIR, 'resources', 'expected', 'mapping.json')

        argv = ['tosql', '--input', self.schema, '--output', self.output]
        main(argv)

        self.assertTrue(filecmp.cmp(self.output, exp))


if __name__ == '__main__':
    unittest.main()
