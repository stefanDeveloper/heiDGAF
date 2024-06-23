import unittest
from unittest.mock import patch, MagicMock

from heidgaf_inspection.inspector import Inspector


class TestInit(unittest.TestCase):
    @patch('heidgaf_inspection.inspector.KafkaConsumeHandler')
    def test_init(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = Inspector()

        self.assertEqual([], sut.messages)
        self.assertEqual(False, sut.busy)
        self.assertEqual(mock_kafka_consume_handler_instance, sut.kafka_consume_handler)
        mock_kafka_consume_handler.assert_called_once_with(topic='Inspect')


class TestGetData(unittest.TestCase):
    @patch('heidgaf_inspection.inspector.KafkaConsumeHandler')
    def test_get_data_without_return_data(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_and_return_json_data.return_value = []

        sut = Inspector()
        sut.get_and_fill_messages()

        self.assertEqual([], sut.messages)
        self.assertEqual(False, sut.busy)

    @patch('heidgaf_inspection.inspector.KafkaConsumeHandler')
    def test_get_data_with_return_data(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_and_return_json_data.return_value = ["test_message_1",
                                                                                         "test_message_2"]

        sut = Inspector()
        sut.busy = False
        sut.get_and_fill_messages()

        self.assertEqual(["test_message_1", "test_message_2"], sut.messages)
        self.assertEqual(True, sut.busy)

    @patch('heidgaf_inspection.inspector.KafkaConsumeHandler')
    def test_get_data_while_busy(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_and_return_json_data.return_value = ["test_message_1",
                                                                                         "test_message_2"]

        sut = Inspector()
        sut.messages = ["test_data"]
        sut.busy = True
        sut.get_and_fill_messages()

        self.assertEqual(["test_data"], sut.messages)
        self.assertEqual(True, sut.busy)

    @patch('heidgaf_inspection.inspector.KafkaConsumeHandler')
    def test_get_data_with_existing_data_to_overwrite(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_and_return_json_data.return_value = []

        sut = Inspector()
        sut.messages = ["test_data"]
        sut.busy = False
        sut.get_and_fill_messages()

        self.assertEqual([], sut.messages)
        self.assertEqual(False, sut.busy)


class TestClearData(unittest.TestCase):
    def test_clear_data_busy_without_existing_data(self):
        sut = Inspector()
        sut.messages = []
        sut.busy = True
        sut.clear_data()

        self.assertEqual([], sut.messages)
        self.assertEqual(False, sut.busy)

    def test_clear_data_busy_with_existing_data(self):
        sut = Inspector()
        sut.messages = ["test_data"]
        sut.busy = True
        sut.clear_data()

        self.assertEqual([], sut.messages)
        self.assertEqual(False, sut.busy)

    def test_clear_data_not_busy_without_existing_data(self):
        sut = Inspector()
        sut.messages = []
        sut.busy = False
        sut.clear_data()

        self.assertEqual([], sut.messages)
        self.assertEqual(False, sut.busy)

    def test_clear_data_not_busy_with_existing_data(self):
        sut = Inspector()
        sut.messages = ["test_data"]
        sut.busy = False
        sut.clear_data()

        self.assertEqual([], sut.messages)
        self.assertEqual(False, sut.busy)


if __name__ == '__main__':
    unittest.main()
