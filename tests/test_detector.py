import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from requests import HTTPError

from src.base import Batch
from src.detector.detector import Detector, WrongChecksum


class TestSha256Sum(unittest.TestCase):
    @patch("src.detector.detector.KafkaConsumeHandler")
    def test_sha256_empty_file(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = Detector()

        with self.assertRaises(FileNotFoundError):
            sut._sha256sum("")

    @patch("src.detector.detector.KafkaConsumeHandler")
    def test_sha256_not_existing_file(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = Detector()

        with self.assertRaises(FileNotFoundError):
            sut._sha256sum("not_existing")


class TestGetModel(unittest.TestCase):
    def setUp(self):
        patcher = patch('src.detector.detector.logger')
        self.mock_logger = patcher.start()
        self.addCleanup(patcher.stop)

    @patch(
        "src.detector.detector.CHECKSUM",
        "21d1f40c9e186a08e9d2b400cea607f4163b39d187a9f9eca3da502b21cf3b9b",
    )
    @patch("src.detector.detector.MODEL", "xg")
    @patch(
        "src.detector.detector.MODEL_BASE_URL",
        "https://heibox.uni-heidelberg.de/d/0d5cbcbe16cd46a58021/",
    )
    @patch("src.detector.detector.KafkaConsumeHandler")
    def test_get_model(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = Detector()

    @patch(
        "src.detector.detector.CHECKSUM",
        "WRONG",
    )
    @patch("src.detector.detector.MODEL", "xg")
    @patch(
        "src.detector.detector.MODEL_BASE_URL",
        "https://heibox.uni-heidelberg.de/d/0d5cbcbe16cd46a58021/",
    )
    @patch("src.detector.detector.KafkaConsumeHandler")
    def test_get_model_not_existing(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        with self.assertRaises(WrongChecksum):
            sut = Detector()

    @patch(
        "src.detector.detector.CHECKSUM",
        "21d1f40c9e186a08e9d2b400cea607f4163b39d187a9f9eca3da502b21cf3b9b",
    )
    @patch("src.detector.detector.MODEL", "WRONG")
    @patch(
        "src.detector.detector.MODEL_BASE_URL",
        "https://heibox.uni-heidelberg.de/d/0d5cbcbe16cd46a58021/",
    )
    @patch("src.detector.detector.KafkaConsumeHandler")
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
    @patch("src.detector.detector.KafkaConsumeHandler")
    def test_get_model_not_existing(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        with self.assertRaises(HTTPError):
            sut = Detector()


class TestInit(unittest.TestCase):
    @patch('src.detector.detector.logger')
    @patch("src.detector.detector.KafkaConsumeHandler")
    def test_init(self, mock_kafka_consume_handler, mock_logger):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = Detector()

        self.assertEqual([], sut.messages)
        self.assertEqual(mock_kafka_consume_handler_instance, sut.kafka_consume_handler)
        mock_kafka_consume_handler.assert_called_once_with(topic="Detector")


class TestGetData(unittest.TestCase):
    @patch('src.detector.detector.logger')
    @patch("src.detector.detector.KafkaConsumeHandler")
    def test_get_data_without_return_data(self, mock_kafka_consume_handler, mock_logger):
        test_batch = Batch(
            begin_timestamp=datetime.now(),
            end_timestamp=datetime.now() + timedelta(0, 3),
            data=[],
        )

        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_and_return_object.return_value = (
            "test",
            test_batch,
        )

        sut = Detector()
        sut.get_and_fill_data()

        self.assertEqual([], sut.messages)

    @patch('src.detector.detector.logger')
    @patch("src.detector.detector.KafkaConsumeHandler")
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
        mock_kafka_consume_handler_instance.consume_and_return_object.return_value = (
            "192.168.1.0/24",
            test_batch,
        )

        sut = Detector()
        sut.messages = []
        sut.get_and_fill_data()

        self.assertEqual(begin, sut.begin_timestamp)
        self.assertEqual(end, sut.end_timestamp)
        self.assertEqual([{"test": "test_message_2"}], sut.messages)

    @patch('src.detector.detector.logger')
    @patch("src.detector.detector.KafkaConsumeHandler")
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


class TestClearData(unittest.TestCase):
    def setUp(self):
        patcher = patch('src.detector.detector.logger')
        self.mock_logger = patcher.start()
        self.addCleanup(patcher.stop)

    @patch('src.detector.detector.logger')
    @patch("src.detector.detector.KafkaConsumeHandler")
    def test_clear_data_without_existing_data(self, mock_kafka_consume_handler, mock_logger):
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

    @patch('src.detector.detector.logger')
    @patch("src.detector.detector.KafkaConsumeHandler")
    def test_clear_data_with_existing_data(self, mock_kafka_consume_handler, mock_logger):
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
