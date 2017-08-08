import unittest

from pymongo_schema.export import *

TEST_DIR = os.path.dirname(__file__)


class TestDataDict(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(os.path.join(TEST_DIR, "resources", "input", "test_schema.json")) as f:
            cls.schema_ex_dict = json.loads(f.read())
        cls.schema_ex_df = mongo_schema_as_dataframe(cls.schema_ex_dict,
                                                     ["Field_compact_name", "Field_name",
                                                      "Full_name", "Description", "Count",
                                                      "Percentage", "Types_count"])

    def setUp(self):
        self.cur_output = None

    def tearDown(self):
        if self.cur_output and not all(sys.exc_info()):
            os.remove(self.cur_output)

    def test01_write_html_tmpl(self):
        self.cur_output = os.path.join(TEST_DIR, "output_data_dict.html")
        expected_file = os.path.join(TEST_DIR, "resources", "expected", "data_dict.html")
        with open(self.cur_output, 'w') as out_fd:
            write_mongo_df_as_html(self.schema_ex_df, out_fd)
        with open(self.cur_output) as out_fd, open(expected_file) as exp_fd:
            self.assertEqual(out_fd.read().replace(' ', ''), exp_fd.read().replace(' ', ''))


if __name__ == '__main__':
    unittest.main()
