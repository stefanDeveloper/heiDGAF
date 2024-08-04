import os
import sys
import unittest

sys.path.append(os.getcwd())
from prefilter.prefilter import create_logger_str


class TestCreateLoggerStr(unittest.TestCase):

    def test_empty_list(self):
        """Test that an empty list returns the correct string."""
        self.assertEqual(create_logger_str([]), "Not filtering by any types.")

    def test_single_entry(self):
        """Test that a single entry returns the correct string."""
        self.assertEqual(create_logger_str(["test"]), "Filtering by type 'test'...")

    def test_multiple_entries(self):
        """Test that multiple entries return the correct string."""
        self.assertEqual(create_logger_str(["test_1", "test_2", "test_3"]),
                         "Filtering by types 'test_1', 'test_2' and 'test_3'...")

    def test_two_entries(self):
        """Test that exactly two entries are formatted correctly."""
        self.assertEqual(create_logger_str(["test_1", "test_2"]),
                         "Filtering by types 'test_1' and 'test_2'...")

    def test_three_entries(self):
        """Test that exactly three entries are formatted correctly."""
        self.assertEqual(create_logger_str(["typeA", "typeB", "typeC"]),
                         "Filtering by types 'typeA', 'typeB' and 'typeC'...")

    def test_long_list(self):
        """Test with a longer list of entries."""
        self.assertEqual(create_logger_str(["apple", "banana", "cherry", "date"]),
                         "Filtering by types 'apple', 'banana', 'cherry' and 'date'...")


if __name__ == '__main__':
    unittest.main()
