import unittest
from unittest.mock import patch, MagicMock

from src.base.logline_handler import (
    RelevanceHandler,
    ListItem,
)

MOCK_REQUIRED_FIELDS = ["ts", "status_code"]


class TestRelevanceHandler(unittest.TestCase):
    def setUp(self):
        self.timestamp_instance = MagicMock(name='ts')
        self.regex_instance = MagicMock(name='record_type')
        self.list_item_instance = ListItem(name='status_code', allowed_list=["200", "300", "404"],relevant_list=['200', '404'])
        self.ip_instance = MagicMock(name='src_ip')

        self.log_config_instances = {
            'ts': self.timestamp_instance,
            'record_type': self.regex_instance,
            'status_code': self.list_item_instance,
            'src_ip': self.ip_instance
        }

        self.handler = RelevanceHandler(log_configuration_instances=self.log_config_instances)

    def test_initialization(self):
        self.assertEqual(self.handler.log_configuration_instances, self.log_config_instances)

    def test_check_relevance_with_valid_function(self):
        # Should call no_relevance_check and return True
        logline = {'status_code': '200'}
        result = self.handler.check_relevance('no_relevance_check', logline)
        self.assertTrue(result)

    def test_check_relevance_with_invalid_function(self):
        logline = {'status_code': '200'}
        with self.assertRaises(Exception) as context:
            self.handler.check_relevance('non_existing_function', logline)
        self.assertIn('Function non_existing_function is not implemented', str(context.exception))

    def test_no_relevance_check_returns_true(self):
        logline = {'status_code': '200'}
        self.assertTrue(self.handler.no_relevance_check(logline))

    def test_check_dga_relevance_all_relevant(self):
        logline = {'status_code': '200'}
        # Should return True because 200 is in relevant_list
        self.assertTrue(self.handler.check_dga_relevance(logline))

    def test_check_dga_relevance_not_relevant(self):
        logline = {'status_code': '500'}
        # Should return False because 500 is not in relevant_list
        self.assertFalse(self.handler.check_dga_relevance(logline))

    def test_check_dga_relevance_empty_list(self):
        self.list_item_instance.relevant_list = []
        logline = {'status_code': '200'}
        # Should return True because relevant_list is empty (no filtering)
        self.assertTrue(self.handler.check_dga_relevance(logline))

if __name__ == "__main__":
    unittest.main()
