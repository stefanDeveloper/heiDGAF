import unittest
from threading import Lock
from unittest.mock import patch, MagicMock

from heidgaf_log_collector import utils
from heidgaf_log_collector.batch_handler import KafkaBatchSender

# placeholders
KAFKA_BROKER_HOST = "localhost"
KAFKA_BROKER_PORT = 9092
BATCH_TIMEOUT = 5.0
BATCH_SIZE = 1000


class TestInit(unittest.TestCase):
    def test_init(self):
        sender_instance = KafkaBatchSender(topic="test_topic")

        self.assertEqual(sender_instance.topic, "test_topic")
        self.assertEqual(sender_instance.messages, [])
        self.assertIsInstance(sender_instance.lock, type(Lock()))
        self.assertIsNone(sender_instance.timer)
        self.assertIsNone(sender_instance.kafka_producer)


class TestStartKafkaProducer(unittest.TestCase):
    @patch('heidgaf_log_collector.batch_handler.Producer')
    def test_start_kafka_producer(self, mock_producer):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        sender_instance = KafkaBatchSender(topic="test_topic")

        sender_instance.start_kafka_producer()

        expected_conf = {'bootstrap.servers': f"{KAFKA_BROKER_HOST}:{KAFKA_BROKER_PORT}"}
        mock_producer.assert_called_once_with(expected_conf)

        self.assertIs(sender_instance.kafka_producer, mock_producer_instance)

    @patch('heidgaf_log_collector.batch_handler.Producer')
    def test_start_kafka_producer_already_running(self, mock_producer):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        sender_instance = KafkaBatchSender(topic="test_topic")

        sender_instance.kafka_producer = MagicMock()

        mock_producer.assert_not_called()


class TestSendBatch(unittest.TestCase):
    @patch('heidgaf_log_collector.batch_handler.Producer')
    def test_send_batch(self, mock_producer):
        mock_producer_instance = MagicMock()
        mock_producer.return_value = mock_producer_instance
        sender_instance = KafkaBatchSender(topic="test_topic")
        sender_instance._reset_timer = MagicMock()
        sender_instance.kafka_producer = mock_producer_instance

        sender_instance.messages = ["message1", "message2"]

        sender_instance._send_batch()

        mock_producer_instance.produce.assert_called_once_with(
            topic="test_topic",
            key=None,
            value=b'["message1", "message2"]',
            callback=utils.kafka_delivery_report,
        )

        mock_producer_instance.flush.assert_called_once()
        sender_instance._reset_timer.assert_called_once()
        self.assertEqual(sender_instance.messages, [])

    def test_send_batch_no_producer(self):
        sender_instance = KafkaBatchSender(topic="test_topic")
        sender_instance._reset_timer = MagicMock()
        sender_instance.kafka_producer = None

        sender_instance.messages = ["message1", "message2"]

        sender_instance._send_batch()

        sender_instance._reset_timer.assert_not_called()
        self.assertEqual(sender_instance.messages, ["message1", "message2"])


class TestClose(unittest.TestCase):
    @patch('heidgaf_log_collector.batch_handler.Timer')
    def test_close_with_active_timer(self, mock_timer):
        sender_instance = KafkaBatchSender(topic="test_topic")
        sender_instance.timer = mock_timer
        sender_instance._send_batch = MagicMock()

        sender_instance.close()

        sender_instance.timer.cancel.assert_called_once()
        sender_instance._send_batch.assert_called_once()

    def test_close_without_timer(self):
        sender_instance = KafkaBatchSender(topic="test_topic")
        sender_instance._send_batch = MagicMock()

        sender_instance.close()

        sender_instance._send_batch.assert_called_once()


class TestResetTimer(unittest.TestCase):
    @patch('heidgaf_log_collector.batch_handler.Timer')
    def test_reset_timer_with_existing_timer(self, mock_timer):
        sender_instance = KafkaBatchSender(topic="test_topic")
        mock_timer_instance = MagicMock()
        sender_instance.timer = mock_timer_instance
        sender_instance._send_batch = MagicMock()
        mock_timer.return_value = mock_timer

        sender_instance._reset_timer()

        mock_timer_instance.cancel.assert_called_once()
        mock_timer.assert_called_once_with(BATCH_TIMEOUT, sender_instance._send_batch)
        self.assertIsNotNone(sender_instance.timer)
        sender_instance.timer.start.assert_called_once()

    @patch('heidgaf_log_collector.batch_handler.Timer')
    def test_reset_timer_without_existing_timer(self, mock_timer):
        sender_instance = KafkaBatchSender(topic="test_topic")
        sender_instance._send_batch = MagicMock()

        sender_instance._reset_timer()

        mock_timer.assert_called_once_with(BATCH_TIMEOUT, sender_instance._send_batch)
        self.assertIsNotNone(sender_instance.timer)
        sender_instance.timer.start.assert_called_once()


class TestAddMessage(unittest.TestCase):
    @patch('heidgaf_log_collector.batch_handler.KafkaBatchSender._send_batch')
    @patch('heidgaf_log_collector.batch_handler.KafkaBatchSender._reset_timer')
    def test_add_message_normal(self, mock_send_batch, mock_reset_timer):
        sender_instance = KafkaBatchSender(topic="test_topic")
        sender_instance.timer = MagicMock()

        sender_instance.add_message("Message")

        mock_send_batch.assert_not_called()
        mock_reset_timer.assert_not_called()

    @patch('heidgaf_log_collector.batch_handler.KafkaBatchSender._send_batch')
    def test_add_message_full_messages(self, mock_send_batch):
        sender_instance = KafkaBatchSender(topic="test_topic")
        sender_instance.timer = MagicMock()

        for i in range(BATCH_SIZE - 1):
            sender_instance.add_message(f"Message {i}")

        mock_send_batch.assert_not_called()

        sender_instance.add_message(f"Message {BATCH_SIZE}")

        mock_send_batch.assert_called_once()

    @patch('heidgaf_log_collector.batch_handler.KafkaBatchSender._reset_timer')
    def test_add_message_no_timer(self, mock_reset_timer):
        sender_instance = KafkaBatchSender(topic="test_topic")
        sender_instance.timer = None

        sender_instance.add_message("Message")
        mock_reset_timer.assert_called_once()


# TODO: Add the rest of the tests

if __name__ == '__main__':
    unittest.main()
