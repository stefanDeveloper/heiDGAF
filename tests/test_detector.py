import os
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, mock_open

from requests import HTTPError

from src.base import Batch
from src.detector.detector import Detector, WrongChecksum


class TestSha256Sum(unittest.TestCase):
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    def test_sha256_empty_file(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = Detector()

        with self.assertRaises(FileNotFoundError):
            sut._sha256sum("")

    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    def test_sha256_not_existing_file(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = Detector()

        with self.assertRaises(FileNotFoundError):
            sut._sha256sum("not_existing")


class TestFeatures(unittest.TestCase):
    def setUp(self):
        patcher = patch("src.detector.detector.logger")
        self.mock_logger = patcher.start()
        self.addCleanup(patcher.stop)

    @patch(
        "src.detector.detector.CHECKSUM",
        "ba1f718179191348fe2abd51644d76191d42a5d967c6844feb3371b6f798bf06",
    )
    @patch("src.detector.detector.MODEL", "rf")
    @patch(
        "src.detector.detector.MODEL_BASE_URL",
        "https://heibox.uni-heidelberg.de/d/0d5cbcbe16cd46a58021/",
    )
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    def test_get_model(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = Detector()
        sut._get_features("google.de")


class TestGetModel(unittest.TestCase):
    def setUp(self):
        patcher = patch("src.detector.detector.logger")
        self.mock_logger = patcher.start()
        self.addCleanup(patcher.stop)

    @patch(
        "src.detector.detector.CHECKSUM",
        "ba1f718179191348fe2abd51644d76191d42a5d967c6844feb3371b6f798bf06",
    )
    @patch("src.detector.detector.MODEL", "rf")
    @patch(
        "src.detector.detector.MODEL_BASE_URL",
        "https://heibox.uni-heidelberg.de/d/0d5cbcbe16cd46a58021/",
    )
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    def test_get_model(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = Detector()

    @patch(
        "src.detector.detector.CHECKSUM",
        "WRONG",
    )
    @patch("src.detector.detector.MODEL", "rf")
    @patch(
        "src.detector.detector.MODEL_BASE_URL",
        "https://heibox.uni-heidelberg.de/d/0d5cbcbe16cd46a58021/",
    )
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    def test_get_model_not_existing(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        with self.assertRaises(WrongChecksum):
            sut = Detector()

    @patch(
        "src.detector.detector.CHECKSUM",
        "04970cd6fe0be5369248d24541c7b8faf69718706019f80280a0a687884f35fb",
    )
    @patch("src.detector.detector.MODEL", "WRONG")
    @patch(
        "src.detector.detector.MODEL_BASE_URL",
        "https://heibox.uni-heidelberg.de/d/0d5cbcbe16cd46a58021/",
    )
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    def test_get_model_not_existing(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        with self.assertRaises(WrongChecksum):
            sut = Detector()

    @patch(
        "src.detector.detector.CHECKSUM",
        "Test",
    )
    @patch("src.detector.detector.MODEL", "xg")
    @patch(
        "src.detector.detector.MODEL_BASE_URL",
        "https://heibox.uni-heidelberg.de/d/WRONG/",
    )
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    def test_get_model_not_existing(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        with self.assertRaises(HTTPError):
            sut = Detector()


class TestInit(unittest.TestCase):
    @patch("src.detector.detector.logger")
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    def test_init(self, mock_kafka_consume_handler, mock_logger):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = Detector()

        self.assertEqual([], sut.messages)
        self.assertEqual(mock_kafka_consume_handler_instance, sut.kafka_consume_handler)
        mock_kafka_consume_handler.assert_called_once_with(topics="Detector")


class TestGetData(unittest.TestCase):
    @patch("src.detector.detector.logger")
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    def test_get_data_without_return_data(
        self, mock_kafka_consume_handler, mock_logger
    ):
        test_batch = Batch(
            begin_timestamp=datetime.now(),
            end_timestamp=datetime.now() + timedelta(0, 3),
            data=[],
        )

        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )

        sut = Detector()
        sut.get_and_fill_data()

        self.assertEqual([], sut.messages)

    @patch("src.detector.detector.logger")
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    def test_get_data_with_return_data(self, mock_kafka_consume_handler, mock_logger):
        begin = datetime.now()
        end = begin + timedelta(0, 3)
        test_batch = Batch(
            begin_timestamp=begin,
            end_timestamp=end,
            data=[{"test": "test_message_2"}],
        )

        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "192.168.1.0/24",
            test_batch,
        )

        sut = Detector()
        sut.messages = []
        sut.get_and_fill_data()

        self.assertEqual(begin, sut.begin_timestamp)
        self.assertEqual(end, sut.end_timestamp)
        self.assertEqual([{"test": "test_message_2"}], sut.messages)

    @patch("src.detector.detector.logger")
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    def test_get_data_while_busy(self, mock_kafka_consume_handler, mock_logger):
        begin = datetime.now()
        end = begin + timedelta(0, 3)
        test_batch = Batch(
            begin_timestamp=begin,
            end_timestamp=end,
            data=[{"test": "test_message_2"}],
        )

        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_and_return_object.return_value = (
            "192.168.1.0/24",
            test_batch,
        )

        sut = Detector()
        sut.messages = [{"test": "test_message_2"}]
        sut.get_and_fill_data()

        self.assertEqual([{"test": "test_message_2"}], sut.messages)


class TestSendWarning(unittest.TestCase):
    def setUp(self):
        patcher = patch("src.detector.detector.logger")
        self.mock_logger = patcher.start()
        self.addCleanup(patcher.stop)

    @patch(
        "src.detector.detector.CHECKSUM",
        "ba1f718179191348fe2abd51644d76191d42a5d967c6844feb3371b6f798bf06",
    )
    @patch("src.detector.detector.MODEL", "rf")
    @patch(
        "src.detector.detector.MODEL_BASE_URL",
        "https://heibox.uni-heidelberg.de/d/0d5cbcbe16cd46a58021/",
    )
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    def test_save_warning(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = Detector()
        sut.warnings = [
            {
                "request": "google.de",
                "probability": 0.8765,
                "model": "rf",
                "sha256": "ba1f718179191348fe2abd51644d76191d42a5d967c6844feb3371b6f798bf06",
            },
            {
                "request": "request.de",
                "probability": 0.12388,
                "model": "rf",
                "sha256": "ba1f718179191348fe2abd51644d76191d42a5d967c6844feb3371b6f798bf06",
            },
        ]
        open_mock = mock_open()
        with patch("src.detector.detector.open", open_mock, create=True):
            sut.send_warning()

        open_mock.assert_called_with(
            os.path.join(tempfile.gettempdir(), "warnings.json"), "a+"
        )

    @patch(
        "src.detector.detector.CHECKSUM",
        "ba1f718179191348fe2abd51644d76191d42a5d967c6844feb3371b6f798bf06",
    )
    @patch("src.detector.detector.MODEL", "rf")
    @patch(
        "src.detector.detector.MODEL_BASE_URL",
        "https://heibox.uni-heidelberg.de/d/0d5cbcbe16cd46a58021/",
    )
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    def test_save_empty_warning(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = Detector()
        sut.warnings = []
        open_mock = mock_open()
        with patch("src.detector.detector.open", open_mock, create=True):
            sut.send_warning()

        open_mock.assert_not_called()

    @patch(
        "src.detector.detector.CHECKSUM",
        "ba1f718179191348fe2abd51644d76191d42a5d967c6844feb3371b6f798bf06",
    )
    @patch("src.detector.detector.MODEL", "rf")
    @patch(
        "src.detector.detector.MODEL_BASE_URL",
        "https://heibox.uni-heidelberg.de/d/0d5cbcbe16cd46a58021/",
    )
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    def test_save_warning_error(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = Detector()
        sut.warnings = [
            {
                "request": "request.de",
                "probability": "INVALID",
                "model": "rf",
                "sha256": "ba1f718179191348fe2abd51644d76191d42a5d967c6844feb3371b6f798bf06",
            }
        ]
        with self.assertRaises(Exception):
            sut.send_warning()


class TestClearData(unittest.TestCase):
    def setUp(self):
        patcher = patch("src.detector.detector.logger")
        self.mock_logger = patcher.start()
        self.addCleanup(patcher.stop)

    @patch("src.detector.detector.logger")
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    def test_clear_data_without_existing_data(
        self, mock_kafka_consume_handler, mock_logger
    ):
        begin = datetime.now()
        end = begin + timedelta(0, 3)
        test_batch = Batch(begin_timestamp=begin, end_timestamp=end, data=[])

        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_and_return_object.return_value = (
            "192.168.1.0/24",
            test_batch,
        )

        sut = Detector()
        sut.messages = []
        sut.clear_data()

        self.assertEqual([], sut.messages)

    @patch("src.detector.detector.logger")
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    def test_clear_data_with_existing_data(
        self, mock_kafka_consume_handler, mock_logger
    ):
        begin = datetime.now()
        end = begin + timedelta(0, 3)
        test_batch = Batch(begin_timestamp=begin, end_timestamp=end, data=[])

        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_and_return_object.return_value = (
            "192.168.1.0/24",
            test_batch,
        )

        sut = Detector()
        sut.messages = ["test_data"]
        sut.begin_timestamp = datetime.now()
        sut.end_timestamp = sut.begin_timestamp + timedelta(0, 3)
        sut.clear_data()

        self.assertEqual([], sut.messages)
        self.assertIsNone(sut.begin_timestamp)
        self.assertIsNone(sut.end_timestamp)


if __name__ == "__main__":
    unittest.main()
