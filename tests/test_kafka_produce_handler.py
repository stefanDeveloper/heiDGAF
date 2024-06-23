import unittest
from unittest.mock import patch, MagicMock

from heidgaf_core.kafka_handler import KafkaProduceHandler


class TestInit(unittest.TestCase):
    @patch('heidgaf_core.kafka_handler.Producer')
    def test_init(self, mock_producer):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        expected_conf = {'bootstrap.servers': 'localhost:9092'}

        handler_instance = KafkaProduceHandler()

        self.assertIsNone(handler_instance.consumer)
        self.assertEqual(mock_producer_instance, handler_instance.producer)
        self.assertEqual("localhost:9092,localhost:9093,localhost:9094",
                         handler_instance.brokers)
        mock_producer.assert_called_once_with(expected_conf)

    @patch('heidgaf_core.kafka_handler.Producer')
    @patch('heidgaf_core.kafka_handler.kafka_delivery_report')
    def test_send_with_data(self, mock_kafka_delivery_report, mock_producer):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance

        handler_instance = KafkaProduceHandler()
        handler_instance.producer = mock_producer_instance
        handler_instance.send("test_topic", "test_data")

        mock_producer_instance.produce.assert_called_once_with(
            topic="test_topic",
            key=None,
            value="test_data".encode('utf-8'),
            callback=mock_kafka_delivery_report,
        )

    @patch('heidgaf_core.kafka_handler.Producer')
    @patch('heidgaf_core.kafka_handler.kafka_delivery_report')
    def test_send_with_empty_data_string(self, mock_kafka_delivery_report, mock_producer):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance

        handler_instance = KafkaProduceHandler()
        handler_instance.producer = mock_producer_instance
        handler_instance.send("test_topic", "")

        mock_producer_instance.produce.assert_called_once_with(
            topic="test_topic",
            key=None,
            value="".encode('utf-8'),
            callback=mock_kafka_delivery_report,
        )


if __name__ == '__main__':
    unittest.main()
