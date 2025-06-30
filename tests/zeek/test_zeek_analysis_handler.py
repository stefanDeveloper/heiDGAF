import unittest
from unittest.mock import patch, MagicMock, call
import os
import subprocess
from src.zeek.zeek_analysis_handler import ZeekAnalysisHandler


class TestZeekAnalysisHandler(unittest.TestCase):

    @patch("src.zeek.zeek_analysis_handler.glob.glob")
    @patch("src.zeek.zeek_analysis_handler.threading.Thread")
    @patch("src.zeek.zeek_analysis_handler.subprocess.run")
    @patch.dict(os.environ, {"STATIC_FILES_DIR": "/mock/static"}, clear=True)
    def test_start_static_analysis(self, mock_run, mock_thread_class, mock_glob):
        mock_glob.return_value = ["/mock/static/test1.pcap", "/mock/static/test2.pcap"]

        # Simulate two separate thread instances
        mock_thread1 = MagicMock()
        mock_thread2 = MagicMock()
        mock_thread_class.side_effect = [mock_thread1, mock_thread2]

        handler = ZeekAnalysisHandler("/mock/config.zeek", "/mock/logs")
        handler.start_static_analysis()

        # Assert threads were created with correct args
        expected_calls = [
            call(target=mock_run, args=(["zeek", "-r", "/mock/static/test1.pcap", "/mock/config.zeek"],)),
            call(target=mock_run, args=(["zeek", "-r", "/mock/static/test2.pcap", "/mock/config.zeek"],))
        ]
        self.assertEqual(mock_thread_class.call_args_list, expected_calls)

        # Assert both threads were started and joined
        mock_thread1.start.assert_called_once()
        mock_thread1.join.assert_called_once()
        mock_thread2.start.assert_called_once()
        mock_thread2.join.assert_called_once()
    @patch("src.zeek.zeek_analysis_handler.subprocess.Popen")
    @patch("src.zeek.zeek_analysis_handler.threading.Thread")
    @patch("src.zeek.zeek_analysis_handler.subprocess.run")
    def test_start_network_analysis(self, mock_run, mock_thread_class, mock_popen):
        # Setup mocks
        mock_process = MagicMock()
        mock_process.stdout.readline.side_effect = ['line1\n', 'line2\n', '']
        mock_popen.return_value = mock_process

        mock_thread_instance = MagicMock()
        mock_thread_class.return_value = mock_thread_instance

        handler = ZeekAnalysisHandler("/mock/config.zeek", "/mock/logs")
        handler.start_network_analysis()

        # Check deploy command was called
        mock_thread_class.assert_any_call(target=mock_run, args=(["zeekctl", "deploy"],))
        mock_thread_instance.start.assert_called()
        mock_thread_instance.join.assert_called()

        # Check tail command started
        mock_popen.assert_called_once_with(
            ["tail", "-f", "/dev/null"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )

        # Ensure a thread was created for reading output
        read_output_call = any("read_output" in str(arg[1]) for arg in mock_thread_class.call_args_list)
        self.assertTrue(read_output_call)


if __name__ == "__main__":
    unittest.main()
