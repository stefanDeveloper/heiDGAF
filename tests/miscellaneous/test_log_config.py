import logging
import unittest
from io import StringIO
from unittest.mock import patch, mock_open

from src.base.log_config import get_logger, load_config


class TestLoggingSetup(unittest.TestCase):

    def setUp(self):
        self.log_stream = StringIO()
        self.handler = logging.StreamHandler(self.log_stream)

    def tearDown(self):
        self.log_stream.close()

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data="logging:\n  base:\n    debug: true\n",
    )
    @patch("yaml.safe_load")
    def test_load_config(self, mock_yaml_load, mock_file):
        mock_yaml_load.return_value = {"logging": {"base": {"debug": True}}}

        config = load_config()

        self.assertIn("logging", config)
        self.assertTrue(config["logging"]["base"]["debug"])

    @patch("src.base.log_config.load_config")
    def test_get_logger_debug_enabled(self, mock_load_config):
        mock_load_config.return_value = {
            "logging": {
                "base": {"debug": False},
                "modules": {"test_module": {"debug": True}},
            }
        }

        logger = get_logger("test_module")
        logger.addHandler(self.handler)

        # Check if logger level is set to DEBUG
        self.assertEqual(logger.level, logging.DEBUG)

        # Log a debug message
        logger.debug("This is a debug message.")
        self.handler.flush()

        log_output = self.log_stream.getvalue()
        self.assertIn("This is a debug message.", log_output)

    @patch("src.base.log_config.load_config")
    def test_get_logger_info_fallback(self, mock_load_config):
        mock_load_config.return_value = {
            "logging": {
                "base": {"debug": False},
                "modules": {"test_module": {"debug": False}},
            }
        }

        logger = get_logger("test_module")
        logger.addHandler(self.handler)

        # Check if logger level is set to INFO
        self.assertEqual(logger.level, logging.INFO)

        # Log messages
        logger.debug("This debug message should not appear.")
        logger.info("This is an info message.")
        self.handler.flush()

        log_output = self.log_stream.getvalue()
        self.assertNotIn("This debug message should not appear.", log_output)
        self.assertIn("This is an info message.", log_output)

    @patch("src.base.log_config.load_config")
    def test_get_logger_default_to_base(self, mock_load_config):
        mock_load_config.return_value = {
            "logging": {"base": {"debug": True}, "modules": {}}
        }

        logger = get_logger("undefined_module")
        logger.addHandler(self.handler)

        # Check if logger level is set to DEBUG based on base config
        self.assertEqual(logger.level, logging.DEBUG)

        logger.debug("Base-level debug message.")
        self.handler.flush()

        log_output = self.log_stream.getvalue()
        self.assertIn("Base-level debug message.", log_output)

    @patch("src.base.log_config.load_config")
    def test_multiple_handler_prevention(self, mock_load_config):
        mock_load_config.return_value = {
            "logging": {"base": {"debug": True}, "modules": {}}
        }

        # Get the logger for the first time
        logger = get_logger("test_module")

        # Call get_logger again for the same module to ensure no additional handler is added
        logger = get_logger("test_module")

        # Ensure there is only one handler
        self.assertEqual(len(logger.handlers), 1)

    @patch("builtins.open", side_effect=FileNotFoundError)
    def test_load_config_file_not_found(self, mock_open):
        with self.assertRaises(FileNotFoundError):
            load_config()


if __name__ == "__main__":
    unittest.main()
