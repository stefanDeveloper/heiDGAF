import unittest
from unittest.mock import patch, MagicMock, call
import os
import tempfile
from src.zeek.zeek_handler import setup_zeek
from click.testing import CliRunner
import yaml
import shutil

class TestZeekHandler(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        self.default_zeek_config = os.path.join(self.temp_dir, "local.zeek")
        self.default_backup = os.path.join(self.temp_dir, "local.zeek_backup")
        
        # Create a dummy default config file
        with open(self.default_zeek_config, "w") as f:
            f.write("# Default Zeek config\n")
    
    def tearDown(self):
        # Clean up temporary directory
        shutil.rmtree(self.temp_dir)
    
    @patch("src.zeek.zeek_handler.ZeekConfigurationHandler")
    @patch("src.zeek.zeek_handler.ZeekAnalysisHandler")
    @patch("builtins.open", new_callable=unittest.mock.mock_open, read_data="config_data")
    @patch("shutil.copy2")
    def test_setup_zeek_success(self, mock_copy, mock_open, mock_analysis_handler, mock_config_handler_cls):
        # Arrange
        mock_config_handler_obj = MagicMock()
        mock_config_handler_obj.zeek_log_location = "/mock/location.log"
        mock_config_handler_obj.is_analysis_static = False
        mock_config_handler_cls.return_value = mock_config_handler_obj

        mock_analysis = MagicMock()
        mock_analysis_handler.return_value = mock_analysis
        
        runner = CliRunner()
        # Act
        result = runner.invoke(setup_zeek, ['-c', '/mock/config.yaml', '--zeek-config-location', '/mock/zeek.cfg'])
        
        # Assert
        mock_config_handler_cls.assert_called_once_with(
            "config_data",
            "/mock/zeek.cfg"
        )
        
        mock_config_handler_cls.return_value.configure.assert_called_once()
        mock_analysis_handler.assert_called_once_with(
            "/mock/zeek.cfg",
            mock_config_handler_obj.zeek_log_location
        )
        mock_analysis.start_analysis.assert_called_once_with(mock_config_handler_obj.is_analysis_static)

    @patch("src.zeek.zeek_handler.ZeekConfigurationHandler")
    @patch("src.zeek.zeek_handler.ZeekAnalysisHandler")
    @patch("builtins.open", new_callable=unittest.mock.mock_open, read_data="config_data")
    @patch("shutil.copy2")
    @patch("yaml.safe_load")
    def test_setup_zeek_with_error(self, mock_yaml_safe_load, mock_copy, mock_open, mock_analysis_handler, mock_config_handler):
        # Arrange
        runner = CliRunner()
        mock_yaml_safe_load.side_effect = yaml.YAMLError
        # Act & Assert
        result = runner.invoke(setup_zeek, ['-c', '/invalid/config.yaml', '--zeek-config-location', '/invalid/zeek.cfg'])
        self.assertIsInstance(result.exception, yaml.YAMLError)
        mock_config_handler.assert_not_called()
        mock_analysis_handler.assert_not_called()
    
    @patch("src.zeek.zeek_handler.ZeekConfigurationHandler")
    @patch("src.zeek.zeek_handler.ZeekAnalysisHandler")
    @patch("builtins.open", new_callable=unittest.mock.mock_open, read_data="config_data")
    @patch("shutil.copy2")
    def test_setup_zeek_static_analysis(self, mock_copy, mock_open, mock_analysis_handler, mock_config_handler_cls):
        # Arrange
        os.environ["STATIC_ANALYSIS"] = "true"
        mock_analysis = MagicMock()
        mock_analysis_handler.return_value = mock_analysis
        
        mock_config_handler_obj = MagicMock()
        mock_config_handler_obj.is_analysis_static = True
        mock_config_handler_cls.return_value = mock_config_handler_obj
        runner = CliRunner()
        # Act
        result = runner.invoke(setup_zeek, ['-c', '/mock/config.yaml', '--zeek-config-location', '/mock/zeek.cfg'])
        # Assert
        mock_analysis.start_analysis.assert_called_once()
        mock_analysis.start_analysis.assert_called_once_with(mock_config_handler_obj.is_analysis_static)
        del os.environ["STATIC_ANALYSIS"]

    @patch("src.zeek.zeek_handler.ZeekConfigurationHandler")
    @patch("src.zeek.zeek_handler.ZeekAnalysisHandler")
    @patch("builtins.open", new_callable=unittest.mock.mock_open, read_data="config_data")
    @patch("shutil.copy2")
    def test_default_config_location_used(self, mock_copy, mock_open, mock_analysis_handler, mock_config_handler_cls):
        """
        Test that the default config location is used when no custom location is provided.
        """
        # Arrange
        mock_config_handler_obj = MagicMock()
        mock_config_handler_obj.zeek_log_location = "/mock/location.log"
        mock_config_handler_obj.is_analysis_static = False
        mock_config_handler_cls.return_value = mock_config_handler_obj

        runner = CliRunner()
        
        # Act
        result = runner.invoke(setup_zeek, ['-c', '/mock/config.yaml'])
        
        # Assert
        # Verify that default location was used
        mock_config_handler_cls.assert_called_once_with(
            "config_data",
            "/usr/local/zeek/share/zeek/site/local.zeek"
        )
        
        # Verify the analysis handler was initialized with the default location
        mock_analysis_handler.assert_called_once_with(
            "/usr/local/zeek/share/zeek/site/local.zeek",
            mock_config_handler_obj.zeek_log_location
        )

    @patch("src.zeek.zeek_handler.ZeekConfigurationHandler")
    @patch("src.zeek.zeek_handler.ZeekAnalysisHandler")
    @patch("builtins.open", new_callable=unittest.mock.mock_open, read_data="config_data")
    @patch("shutil.copy2")
    def test_custom_config_location_used(self, mock_copy, mock_open, mock_analysis_handler, mock_config_handler_cls):
        """
        Test that a custom config location is used when provided.
        """
        # Arrange
        mock_config_handler_obj = MagicMock()
        mock_config_handler_obj.zeek_log_location = "/mock/location.log"
        mock_config_handler_obj.is_analysis_static = False
        mock_config_handler_cls.return_value = mock_config_handler_obj

        runner = CliRunner()
        
        # Act
        result = runner.invoke(setup_zeek, ['-c', '/mock/config.yaml', '--zeek-config-location', '/custom/zeek.cfg'])
        
        # Assert
        # Verify that custom location was used
        mock_config_handler_cls.assert_called_once_with(
            "config_data",
            "/custom/zeek.cfg"
        )
        
        # Verify the analysis handler was initialized with the custom location
        mock_analysis_handler.assert_called_once_with(
            "/custom/zeek.cfg",
            mock_config_handler_obj.zeek_log_location
        )

    @patch("src.zeek.zeek_handler.ZeekConfigurationHandler")
    @patch("src.zeek.zeek_handler.ZeekAnalysisHandler")
    @patch("builtins.open", new_callable=unittest.mock.mock_open, read_data="config_data")
    @patch("os.path.isfile")
    @patch("shutil.copy2")
    def test_non_initial_setup_restore_backup(self, mock_copy, mock_isfile, mock_open, mock_analysis_handler, mock_config_handler_cls):
        """
        Test that the backup config is restored when it's not the initial setup.
        """
        # Arrange
        runner = CliRunner()
        
        # Mock os.path.isfile to return True, indicating backup exists (non-initial setup)
        mock_isfile.return_value = True
        
        # Mock environment
        mock_config_handler_obj = MagicMock()
        mock_config_handler_obj.zeek_log_location = "/mock/location.log"
        mock_config_handler_obj.is_analysis_static = False
        mock_config_handler_cls.return_value = mock_config_handler_obj

        # Act
        result = runner.invoke(setup_zeek, ['-c', '/mock/config.yaml'])
        
        # Assert
        # Verify that the backup was restored
        mock_copy.assert_any_call(
            "/opt/local.zeek_backup",
            "/usr/local/zeek/share/zeek/site/local.zeek"
        )
        
        # Verify configuration proceeded after restore
        mock_config_handler_cls.return_value.configure.assert_called_once()

    @patch("src.zeek.zeek_handler.ZeekConfigurationHandler")
    @patch("src.zeek.zeek_handler.ZeekAnalysisHandler")
    @patch("builtins.open", new_callable=unittest.mock.mock_open, read_data="config_data")
    @patch("os.path.isfile")
    @patch("shutil.copy2")
    def test_initial_setup_backup_default(self, mock_copy, mock_isfile, mock_open, mock_analysis_handler, mock_config_handler_cls):
        """
        Test that the default config is backed up when it's the initial setup.
        """
        # Arrange
        runner = CliRunner()
        
        # Mock os.path.isfile to return False, indicating no backup exists (initial setup)
        mock_isfile.return_value = False
        
        # Mock environment
        mock_config_handler_obj = MagicMock()
        mock_config_handler_obj.zeek_log_location = "/mock/location.log"
        mock_config_handler_obj.is_analysis_static = False
        mock_config_handler_cls.return_value = mock_config_handler_obj

        # Act
        result = runner.invoke(setup_zeek, ['-c', '/mock/config.yaml'])
        
        # Assert
        # Verify that the default config was backed up
        mock_copy.assert_any_call(
            "/usr/local/zeek/share/zeek/site/local.zeek",
            "/opt/local.zeek_backup"
        )
        
        # Verify configuration proceeded after backup
        mock_config_handler_cls.return_value.configure.assert_called_once()