import unittest
from unittest.mock import patch, MagicMock

from heidgaf_core.kafka_handler import KafkaProduceHandler


class TestInit(unittest.TestCase):
    @patch('heidgaf_core.kafka_handler.Producer')
    def test_init(self, mock_producer):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance

        expected_conf = {
            'bootstrap.servers': "localhost:8097,localhost:8098,localhost:8099",
            'transactional.id': "test_transactional_id"
        }

        sut = KafkaProduceHandler(transactional_id="test_transactional_id")

        self.assertIsNone(sut.consumer)
        self.assertEqual("localhost:8097,localhost:8098,localhost:8099",
                         sut.brokers)
        self.assertEqual(mock_producer_instance, sut.producer)

        mock_producer.assert_called_once_with(expected_conf)
        mock_producer_instance.init_transactions.assert_called_once()


class TestSend(unittest.TestCase):
    @patch('heidgaf_core.kafka_handler.Producer')
    @patch('heidgaf_core.kafka_handler.KafkaProduceHandler.commit_transaction_with_retry')
    @patch('heidgaf_core.kafka_handler.kafka_delivery_report')
    def test_send_with_data(self, mock_kafka_delivery_report, mock_commit_transaction_with_retry, mock_producer):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance

        sut = KafkaProduceHandler(transactional_id="test_transactional_id")
        sut.send("test_topic", "test_data")

        mock_producer_instance.produce.assert_called_once_with(
            topic="test_topic",
            key=None,
            value="test_data".encode('utf-8'),
            callback=mock_kafka_delivery_report,
        )
        mock_commit_transaction_with_retry.assert_called_once()
        mock_producer_instance.begin_transaction.assert_called_once()

    @patch('heidgaf_core.kafka_handler.Producer')
    def test_send_with_empty_data_string(self, mock_producer):
        sut = KafkaProduceHandler(transactional_id="test_transactional_id")
        sut.send("test_topic", "")

        mock_producer.begin_transaction.assert_not_called()
        mock_producer.produce.assert_not_called()
        mock_producer.commit_transaction_with_retry.assert_not_called()


if __name__ == '__main__':
    unittest.main()
