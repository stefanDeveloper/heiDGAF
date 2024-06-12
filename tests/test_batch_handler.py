import json
import unittest
from unittest.mock import patch, MagicMock

from heidgaf_core.batch_handler import KafkaBatchSender
from heidgaf_core.config import *


class TestInit(unittest.TestCase):
    @patch('heidgaf_core.batch_handler.KafkaProduceHandler')
    @patch('heidgaf_core.batch_handler.Lock')
    def test_init(self, mock_lock, mock_kafka_produce_handler):
        mock_lock_instance = MagicMock()
        mock_lock.return_value = mock_lock_instance
        mock_handler_instance = MagicMock()
        mock_kafka_produce_handler.return_value = mock_handler_instance
        sender_instance = KafkaBatchSender(topic="test_topic")

        self.assertEqual(sender_instance.topic, "test_topic")
        self.assertEqual(sender_instance.messages, [])
        self.assertIsNone(sender_instance.timer)
        mock_lock.assert_called_once()
        mock_kafka_produce_handler.assert_called_once()
        self.assertEqual(mock_handler_instance, sender_instance.kafka_produce_handler)
        self.assertEqual(mock_lock_instance, sender_instance.lock)


class TestSendBatch(unittest.TestCase):
    @patch('heidgaf_core.batch_handler.KafkaProduceHandler')
    def test_send_batch_with_messages(self, mock_kafka_produce_handler):
        sender_instance = KafkaBatchSender(topic="test_topic")
        sender_instance._reset_timer = MagicMock()
        mock_handler_instance = mock_kafka_produce_handler.return_value
        mock_send = mock_handler_instance.send

        sender_instance.messages = ["message1", "message2"]

        sender_instance._send_batch()

        sender_instance._reset_timer.assert_called_once()
        self.assertEqual(sender_instance.messages, [])
        mock_send.assert_called_once_with(
            topic="test_topic",
            data=json.dumps(["message1", "message2"])
        )

    def test_send_batch_without_messages(self):
        sender_instance = KafkaBatchSender(topic="test_topic")
        sender_instance._reset_timer = MagicMock()

        sender_instance.messages = []

        sender_instance._send_batch()

        sender_instance._reset_timer.assert_called_once()
        self.assertEqual(sender_instance.messages, [])


class TestClose(unittest.TestCase):
    @patch('heidgaf_core.batch_handler.Timer')
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
    @patch('heidgaf_core.batch_handler.Timer')
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

    @patch('heidgaf_core.batch_handler.Timer')
    def test_reset_timer_without_existing_timer(self, mock_timer):
        sender_instance = KafkaBatchSender(topic="test_topic")
        sender_instance._send_batch = MagicMock()

        sender_instance._reset_timer()

        mock_timer.assert_called_once_with(BATCH_TIMEOUT, sender_instance._send_batch)
        self.assertIsNotNone(sender_instance.timer)
        sender_instance.timer.start.assert_called_once()


class TestAddMessage(unittest.TestCase):
    @patch('heidgaf_core.batch_handler.KafkaBatchSender._send_batch')
    @patch('heidgaf_core.batch_handler.KafkaBatchSender._reset_timer')
    def test_add_message_normal(self, mock_send_batch, mock_reset_timer):
        sender_instance = KafkaBatchSender(topic="test_topic")
        sender_instance.timer = MagicMock()

        sender_instance.add_message("Message")

        mock_send_batch.assert_not_called()
        mock_reset_timer.assert_not_called()

    @patch('heidgaf_core.batch_handler.KafkaBatchSender._send_batch')
    def test_add_message_full_messages(self, mock_send_batch):
        sender_instance = KafkaBatchSender(topic="test_topic")
        sender_instance.timer = MagicMock()

        for i in range(BATCH_SIZE - 1):
            sender_instance.add_message(f"Message {i}")

        mock_send_batch.assert_not_called()

        sender_instance.add_message(f"Message {BATCH_SIZE}")

        mock_send_batch.assert_called_once()

    @patch('heidgaf_core.batch_handler.KafkaBatchSender._reset_timer')
    def test_add_message_no_timer(self, mock_reset_timer):
        sender_instance = KafkaBatchSender(topic="test_topic")
        sender_instance.timer = None

        sender_instance.add_message("Message")
        mock_reset_timer.assert_called_once()


# TODO: Add the rest of the tests

if __name__ == '__main__':
    unittest.main()
