import unittest
from unittest.mock import MagicMock, patch

from confluent_kafka import KafkaException

from src.base.kafka_handler import ExactlyOnceKafkaProduceHandler


class TestInit(unittest.TestCase):
    @patch("src.base.kafka_handler.HOSTNAME", "test_transactional_id")
    @patch(
        "src.base.kafka_handler.KAFKA_BROKERS",
        [
            {
                "hostname": "127.0.0.1",
                "port": 9999,
            },
            {
                "hostname": "127.0.0.2",
                "port": 9998,
            },
            {
                "hostname": "127.0.0.3",
                "port": 9997,
            },
        ],
    )
    @patch("src.base.kafka_handler.uuid")
    @patch("src.base.kafka_handler.Producer")
    def test_init(self, mock_producer, mock_uuid):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        mock_uuid.uuid4.return_value = "fixed‑uuid‑1234‑abcd‑5678‑90ef"
        expected_conf = {
            "bootstrap.servers": "127.0.0.1:9999,127.0.0.2:9998,127.0.0.3:9997",
            "transactional.id": f"test_transactional_id-{mock_uuid.uuid4.return_value}",
            "enable.idempotence": True,
            'message.max.bytes': 1000000000,
        }

        sut = ExactlyOnceKafkaProduceHandler()

        self.assertIsNone(sut.consumer)
        self.assertEqual(mock_producer_instance, sut.producer)

        mock_producer.assert_called_once_with(expected_conf)
        mock_producer_instance.init_transactions.assert_called_once()

    @patch("src.base.kafka_handler.uuid")
    @patch("src.base.kafka_handler.logger")
    @patch(
        "src.base.kafka_handler.KAFKA_BROKERS",
        [
            {
                "hostname": "127.0.0.1",
                "port": 9999,
            },
            {
                "hostname": "127.0.0.2",
                "port": 9998,
            },
            {
                "hostname": "127.0.0.3",
                "port": 9997,
            },
        ],
    )
    @patch("src.base.kafka_handler.Producer")
    def test_init_fail(self, mock_producer, mock_logger, mock_uuid):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        mock_uuid.uuid4.return_value = "fixed‑uuid‑1234‑abcd‑5678‑90ef"

        expected_conf = {
            "bootstrap.servers": "127.0.0.1:9999,127.0.0.2:9998,127.0.0.3:9997",
            "transactional.id": f"default_tid-{mock_uuid.uuid4.return_value}",
            "enable.idempotence": True,
            "message.max.bytes": 1000000000
        }

        with patch.object(
            mock_producer_instance, "init_transactions", side_effect=KafkaException
        ):
            with self.assertRaises(KafkaException):
                sut = ExactlyOnceKafkaProduceHandler()

            mock_producer.assert_called_once_with(expected_conf)
            mock_producer_instance.init_transactions.assert_called_once()


class TestSend(unittest.TestCase):
    @patch(
        "src.base.kafka_handler.KAFKA_BROKERS",
        [
            {
                "hostname": "127.0.0.1",
                "port": 9999,
            },
            {
                "hostname": "127.0.0.2",
                "port": 9998,
            },
            {
                "hostname": "127.0.0.3",
                "port": 9997,
            },
        ],
    )
    @patch("src.base.kafka_handler.Producer")
    @patch(
        "src.base.kafka_handler.ExactlyOnceKafkaProduceHandler.commit_transaction_with_retry"
    )
    @patch("src.base.kafka_handler.kafka_delivery_report")
    def test_send_with_data(
        self,
        mock_kafka_delivery_report,
        mock_commit_transaction_with_retry,
        mock_producer,
    ):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance

        sut = ExactlyOnceKafkaProduceHandler()
        sut.produce("test_topic", "test_data", key=None)

        mock_producer_instance.produce.assert_called_once_with(
            topic="test_topic",
            key=None,
            value="test_data",
            callback=mock_kafka_delivery_report,
        )
        mock_commit_transaction_with_retry.assert_called_once()
        mock_producer_instance.begin_transaction.assert_called_once()

    @patch(
        "src.base.kafka_handler.KAFKA_BROKERS",
        [
            {
                "hostname": "127.0.0.1",
                "port": 9999,
            },
            {
                "hostname": "127.0.0.2",
                "port": 9998,
            },
            {
                "hostname": "127.0.0.3",
                "port": 9997,
            },
        ],
    )
    @patch("src.base.kafka_handler.Producer")
    def test_send_with_empty_data_string(self, mock_producer):
        sut = ExactlyOnceKafkaProduceHandler()
        sut.produce("test_topic", "", None)

        mock_producer.begin_transaction.assert_not_called()
        mock_producer.produce.assert_not_called()
        mock_producer.commit_transaction_with_retry.assert_not_called()

    @patch("src.base.kafka_handler.logger")
    @patch(
        "src.base.kafka_handler.KAFKA_BROKERS",
        [
            {
                "hostname": "127.0.0.1",
                "port": 9999,
            },
            {
                "hostname": "127.0.0.2",
                "port": 9998,
            },
            {
                "hostname": "127.0.0.3",
                "port": 9997,
            },
        ],
    )
    @patch("src.base.kafka_handler.Producer")
    @patch("src.base.kafka_handler.kafka_delivery_report")
    @patch(
        "src.base.kafka_handler.ExactlyOnceKafkaProduceHandler.commit_transaction_with_retry"
    )
    def test_send_fail(
        self,
        mock_commit_transaction_with_retry,
        mock_kafka_delivery_report,
        mock_producer,
        mock_logger,
    ):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        mock_commit_transaction_with_retry.side_effect = Exception

        sut = ExactlyOnceKafkaProduceHandler()

        with self.assertRaises(Exception):
            sut.produce("test_topic", "test_data", key=None)

        mock_producer_instance.produce.assert_called_once_with(
            topic="test_topic",
            key=None,
            value="test_data",
            callback=mock_kafka_delivery_report,
        )

        mock_producer_instance.abort_transaction.assert_called_once()
        mock_producer_instance.begin_transaction.assert_called_once()


class TestCommitTransactionWithRetry(unittest.TestCase):
    # def test_commit_transaction_with_retry(self):
    @patch(
        "src.base.kafka_handler.KAFKA_BROKERS",
        [
            {
                "hostname": "127.0.0.1",
                "port": 9999,
            },
            {
                "hostname": "127.0.0.2",
                "port": 9998,
            },
            {
                "hostname": "127.0.0.3",
                "port": 9997,
            },
        ],
    )
    @patch("src.base.kafka_handler.Producer")
    @patch("time.sleep", return_value=None)
    def test_commit_successful(self, mock_sleep, mock_producer):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        mock_producer.commit_transaction.return_value = None

        sut = ExactlyOnceKafkaProduceHandler()
        sut.commit_transaction_with_retry()

        mock_producer_instance.commit_transaction.assert_called_once()
        mock_sleep.assert_not_called()

    @patch(
        "src.base.kafka_handler.KAFKA_BROKERS",
        [
            {
                "hostname": "127.0.0.1",
                "port": 9999,
            },
            {
                "hostname": "127.0.0.2",
                "port": 9998,
            },
            {
                "hostname": "127.0.0.3",
                "port": 9997,
            },
        ],
    )
    @patch("src.base.kafka_handler.Producer")
    @patch("time.sleep", return_value=None)
    def test_commit_retries_then_successful(self, mock_sleep, mock_producer):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        mock_producer_instance.commit_transaction.side_effect = [
            KafkaException(
                "Conflicting commit_transaction API call is already in progress"
            ),
            None,
        ]

        sut = ExactlyOnceKafkaProduceHandler()
        sut.commit_transaction_with_retry()

        self.assertEqual(mock_producer_instance.commit_transaction.call_count, 2)
        mock_sleep.assert_called_once_with(1.0)

    @patch("src.base.kafka_handler.logger")
    @patch(
        "src.base.kafka_handler.KAFKA_BROKERS",
        [
            {
                "hostname": "127.0.0.1",
                "port": 9999,
            },
            {
                "hostname": "127.0.0.2",
                "port": 9998,
            },
            {
                "hostname": "127.0.0.3",
                "port": 9997,
            },
        ],
    )
    @patch("src.base.kafka_handler.Producer")
    @patch("time.sleep", return_value=None)
    def test_commit_retries_and_fails(self, mock_sleep, mock_producer, mock_logger):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        mock_producer_instance.commit_transaction.side_effect = KafkaException(
            "Conflicting commit_transaction API call is already in progress"
        )

        sut = ExactlyOnceKafkaProduceHandler()
        with self.assertRaises(RuntimeError) as context:
            sut.commit_transaction_with_retry()

        self.assertEqual(mock_producer_instance.commit_transaction.call_count, 3)
        self.assertEqual(
            str(context.exception), "Failed to commit transaction after retries."
        )
        self.assertEqual(mock_sleep.call_count, 3)

    @patch(
        "src.base.kafka_handler.KAFKA_BROKERS",
        [
            {
                "hostname": "127.0.0.1",
                "port": 9999,
            },
            {
                "hostname": "127.0.0.2",
                "port": 9998,
            },
            {
                "hostname": "127.0.0.3",
                "port": 9997,
            },
        ],
    )
    @patch("src.base.kafka_handler.Producer")
    @patch("time.sleep", return_value=None)
    def test_commit_fails_with_other_exception(self, mock_sleep, mock_producer):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        mock_producer_instance.commit_transaction.side_effect = KafkaException(
            "Some other error"
        )

        sut = ExactlyOnceKafkaProduceHandler()
        with self.assertRaises(KafkaException) as context:
            sut.commit_transaction_with_retry()

        mock_producer_instance.commit_transaction.assert_called_once()
        self.assertEqual(str(context.exception), "Some other error")
        mock_sleep.assert_not_called()


class TestDel(unittest.TestCase):
    @patch(
        "src.base.kafka_handler.KAFKA_BROKERS",
        [
            {
                "hostname": "127.0.0.1",
                "port": 9999,
            },
            {
                "hostname": "127.0.0.2",
                "port": 9998,
            },
            {
                "hostname": "127.0.0.3",
                "port": 9997,
            },
        ],
    )
    @patch("src.base.kafka_handler.Producer")
    def test_del(self, mock_producer):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance

        sut = ExactlyOnceKafkaProduceHandler()
        del sut

        mock_producer_instance.flush.assert_called_once()


if __name__ == "__main__":
    unittest.main()
