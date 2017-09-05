import unittest

import sys

import os


class MockUnittestPy3Outcom(object):
    errors = [[None]]


class TestRemovingOutputOnSuccess(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._outcome = MockUnittestPy3Outcom

    def setUp(self):
        self.output = None

    def tearDown(self):
        if self.output and not all(sys.exc_info()) \
                and self._outcome.errors and self._outcome.errors[-1][-1] is None:
            if isinstance(self.output, str):
                os.remove(self.output)
            else:
                for output in self.output:
                    os.remove(output)
