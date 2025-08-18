import unittest
from unittest.mock import patch, MagicMock
from src.zeek.zeek_analysis_handler import ZeekAnalysisHandler
import os
class TestZeekAnalysisHandler(unittest.TestCase):
    def setUp(self):
        os.environ["STATIC_FILES_DIR"] = "/tmp"
        self.handler = ZeekAnalysisHandler("/mock/config.zeek", "/mock/logs")
    def tearDown(self):
        del os.environ["STATIC_FILES_DIR"]
    @patch("src.zeek.zeek_analysis_handler.glob.glob")
    @patch("src.zeek.zeek_analysis_handler.threading.Thread")
    def test_start_static_analysis(self, mock_thread_class, mock_glob):
        mock_glob.return_value = ["/tmp/test.pcap"]
        mock_thread_instance = MagicMock()
        mock_thread_instance.start = MagicMock()
        mock_thread_class.return_value = mock_thread_instance

        # Act
        self.handler.start_static_analysis()
        call, args = mock_thread_class.call_args_list[0]
        args_list = args["args"]
        # Assert
        self.assertIn( ["zeek", "-r", "/tmp/test.pcap", "/mock/config.zeek"],args_list)
        mock_thread_class.assert_called_once()
        mock_thread_instance.start.assert_called()
        mock_thread_instance.join.assert_called()

    @patch("src.zeek.zeek_analysis_handler.threading.Thread")
    def test_start_analysis_network_mode(self, mock_thread):
        # Act
        self.handler.start_analysis(static_analysis=False)
        self.handler.start_static_analysis = MagicMock()

        # Assert
        self.handler.start_static_analysis.assert_not_called()

    @patch("src.zeek.zeek_analysis_handler.threading.Thread")
    def test_start_analysis_static_mode(self, mock_thread):
        # Act
        self.handler.start_analysis(static_analysis=True)
        self.handler.start_network_analysis = MagicMock()
        
        # Assert
        self.handler.start_network_analysis.assert_not_called()