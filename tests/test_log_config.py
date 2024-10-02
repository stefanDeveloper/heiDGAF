import logging
import os
import sys
import unittest
from io import StringIO
from unittest.mock import patch, mock_open

sys.path.append(os.getcwd())
from src.base.log_config import setup_logging


class TestLoggingSetup(unittest.TestCase):

    def setUp(self):
        """
        Prepare the test environment by setting up a log capture stream.
        """
        self.log_stream = StringIO()
        self.handler = logging.StreamHandler(self.log_stream)
        self.logger = logging.getLogger()

        self.logger.handlers = []
        self.logger.addHandler(self.handler)
        self.logger.setLevel(logging.DEBUG)

    def tearDown(self):
        """
        Clean up after each test.
        """
        self.logger.removeHandler(self.handler)
        self.log_stream.close()

    @patch(
        "builtins.open", new_callable=mock_open, read_data="logging:\n  debug: true\n"
    )
    @patch("yaml.safe_load")
    def test_debug_logging_enabled(self, mock_yaml_load, mock_file):
        mock_yaml_load.return_value = {"logging": {"debug": True}}

        setup_logging()

        self.assertEqual(self.logger.level, logging.DEBUG)

    @patch(
        "builtins.open", new_callable=mock_open, read_data="logging:\n  debug: false\n"
    )
    @patch("yaml.safe_load")
    def test_info_logging_enabled(self, mock_yaml_load, mock_file):
        mock_yaml_load.return_value = {"logging": {"debug": False}}

        setup_logging()

        self.assertEqual(self.logger.level, logging.INFO)

    @patch("builtins.open", new_callable=mock_open)
    @patch("yaml.safe_load", side_effect=FileNotFoundError)
    def test_config_file_not_found(self, mock_yaml_load, mock_file):
        with self.assertRaises(FileNotFoundError):
            setup_logging()

    @patch(
        "builtins.open", new_callable=mock_open, read_data="logging:\n  debug: true\n"
    )
    @patch("yaml.safe_load")
    def test_handler_replacement(self, mock_yaml_load, mock_file):
        mock_yaml_load.return_value = {"logging": {"debug": True}}

        setup_logging()

        setup_logging()

        self.assertEqual(len(self.logger.handlers), 1)
        self.assertIsInstance(self.logger.handlers[0], logging.StreamHandler)


if __name__ == "__main__":
    unittest.main()
