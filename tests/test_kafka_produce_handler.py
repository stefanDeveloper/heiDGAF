import unittest
from unittest.mock import patch, MagicMock

from confluent_kafka import KafkaException

from heidgaf_core.kafka_handler import KafkaProduceHandler

BROKER_1 = "172.27.0.3:8097"
BROKER_2 = "172.27.0.4:8098"
BROKER_3 = "172.27.0.5:8099"

class TestInit(unittest.TestCase):
    @patch('heidgaf_core.kafka_handler.Producer')
    def test_init(self, mock_producer):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance

        expected_conf = {
            'bootstrap.servers': f"{BROKER_1},{BROKER_2},{BROKER_3}",
            'transactional.id': "test_transactional_id"
        }

        sut = KafkaProduceHandler(transactional_id="test_transactional_id")

        self.assertIsNone(sut.consumer)
        self.assertEqual(f"{BROKER_1},{BROKER_2},{BROKER_3}",
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


class TestCommitTransactionWithRetry(unittest.TestCase):
    # def test_commit_transaction_with_retry(self):
    @patch('heidgaf_core.kafka_handler.Producer')
    @patch('time.sleep', return_value=None)
    def test_commit_successful(self, mock_sleep, mock_producer):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        mock_producer.commit_transaction.return_value = None

        sut = KafkaProduceHandler(transactional_id="test_transactional_id")
        sut.commit_transaction_with_retry()

        mock_producer_instance.commit_transaction.assert_called_once()
        mock_sleep.assert_not_called()

    @patch('heidgaf_core.kafka_handler.Producer')
    @patch('time.sleep', return_value=None)
    def test_commit_retries_then_successful(self, mock_sleep, mock_producer):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        mock_producer_instance.commit_transaction.side_effect = [
            KafkaException("Conflicting commit_transaction API call is already in progress"),
            None
        ]

        sut = KafkaProduceHandler(transactional_id="test_transactional_id")
        sut.commit_transaction_with_retry()

        self.assertEqual(mock_producer_instance.commit_transaction.call_count, 2)
        mock_sleep.assert_called_once_with(1.0)

    @patch('heidgaf_core.kafka_handler.Producer')
    @patch('time.sleep', return_value=None)
    def test_commit_retries_and_fails(self, mock_sleep, mock_producer):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        mock_producer_instance.commit_transaction.side_effect = KafkaException(
            "Conflicting commit_transaction API call is already in progress"
        )

        sut = KafkaProduceHandler(transactional_id="test_transactional_id")
        with self.assertRaises(RuntimeError) as context:
            sut.commit_transaction_with_retry()

        self.assertEqual(mock_producer_instance.commit_transaction.call_count, 3)
        self.assertEqual(str(context.exception), "Failed to commit transaction after retries")
        self.assertEqual(mock_sleep.call_count, 3)

    @patch('heidgaf_core.kafka_handler.Producer')
    @patch('time.sleep', return_value=None)
    def test_commit_fails_with_other_exception(self, mock_sleep, mock_producer):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        mock_producer_instance.commit_transaction.side_effect = KafkaException("Some other error")

        sut = KafkaProduceHandler(transactional_id="test_transactional_id")
        with self.assertRaises(KafkaException) as context:
            sut.commit_transaction_with_retry()

        mock_producer_instance.commit_transaction.assert_called_once()
        self.assertEqual(str(context.exception), "Some other error")
        mock_sleep.assert_not_called()


class TestClose(unittest.TestCase):
    @patch('heidgaf_core.kafka_handler.Producer')
    def test_close(self, mock_producer):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance

        sut = KafkaProduceHandler(transactional_id="test_transactional_id")
        sut.close()

        mock_producer_instance.flush.assert_called_once()


if __name__ == '__main__':
    unittest.main()
