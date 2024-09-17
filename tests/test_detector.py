import unittest
from unittest.mock import MagicMock, patch

from src.detector.detector import Detector, main


class TestInit(unittest.TestCase):
    @patch("src.detector.detector.KafkaConsumeHandler")
    def test_init(self, mock_kafka_consume_handler, mock_produce_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = Detector()

        self.assertEqual([], sut.messages)
        self.assertEqual(mock_kafka_consume_handler_instance, sut.kafka_consume_handler)
        mock_kafka_consume_handler.assert_called_once_with(topic="Inspect")


class TestGetData(unittest.TestCase):
    @patch("src.detector.detector.KafkaConsumeHandler")
    def test_get_data_without_return_data(
        self, mock_kafka_consume_handler, mock_produce_handler
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_and_return_json_data.return_value = (
            None,
            {},
        )
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Detector()
        sut.get_and_fill_data()

        self.assertEqual([], sut.messages)

    @patch("src.detector.detector.KafkaConsumeHandler")
    def test_get_data_with_return_data(
        self, mock_kafka_consume_handler, mock_produce_handler
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_and_return_json_data.return_value = "192.168.1.0/24", {
            "begin_timestamp": "2024-05-21T08:31:27.000Z",
            "end_timestamp": "2024-05-21T08:31:29.000Z",
            "data": [
                "test_message_1",
                "test_message_2",
            ],
        }

        sut = Detector()
        sut.messages = []
        sut.get_and_fill_data()

        self.assertEqual("2024-05-21T08:31:27.000Z", sut.begin_timestamp)
        self.assertEqual("2024-05-21T08:31:29.000Z", sut.end_timestamp)
        self.assertEqual(["test_message_1", "test_message_2"], sut.messages)

    @patch("src.detector.detector.KafkaConsumeHandler")
    def test_get_data_while_busy(
        self, mock_kafka_consume_handler, mock_produce_handler
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_and_return_json_data.return_value = "172.126.0.0/24", {
            "begin_timestamp": "2024-05-21T08:31:27.000Z",
            "end_timestamp": "2024-05-21T08:31:29.000Z",
            "data": [
                "test_message_1",
                "test_message_2",
            ],
        }

        sut = Detector()
        sut.messages = ["test_data"]
        sut.get_and_fill_data()

        self.assertEqual(["test_data"], sut.messages)


class TestClearData(unittest.TestCase):
    @patch("src.detector.detector.KafkaConsumeHandler")
    def test_clear_data_without_existing_data(
        self, mock_kafka_consume_handler, mock_produce_handler
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_and_return_json_data.return_value = "172.126.0.0/24", {
            "begin_timestamp": "2024-05-21T08:31:27.000Z",
            "end_timestamp": "2024-05-21T08:31:29.000Z",
            "data": [],
        }

        sut = Detector()
        sut.messages = []
        sut.clear_data()

        self.assertEqual([], sut.messages)

    @patch("src.detector.detector.KafkaConsumeHandler")
    def test_clear_data_with_existing_data(
        self, mock_kafka_consume_handler, mock_produce_handler
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_and_return_json_data.return_value = "172.126.0.0/24", {
            "begin_timestamp": "2024-05-21T08:31:27.000Z",
            "end_timestamp": "2024-05-21T08:31:29.000Z",
            "data": [],
        }

        sut = Detector()
        sut.messages = ["test_data"]
        sut.begin_timestamp = "2024-05-21T08:31:27.000Z"
        sut.end_timestamp = "2024-05-21T08:31:29.000Z"
        sut.clear_data()

        self.assertEqual([], sut.messages)
        self.assertIsNone(sut.begin_timestamp)
        self.assertIsNone(sut.end_timestamp)


if __name__ == "__main__":
    unittest.main()
