import json
import unittest
from unittest.mock import patch, MagicMock

from heidgaf_core.batch_handler import KafkaBatchSender
from heidgaf_core.config import *


class TestInit(unittest.TestCase):
    @patch('heidgaf_core.batch_handler.KafkaProduceHandler')
    @patch('heidgaf_core.batch_handler.Lock')
    def test_init_without_puffer(self, mock_lock, mock_kafka_produce_handler):
        mock_lock_instance = MagicMock()
        mock_lock.return_value = mock_lock_instance
        mock_handler_instance = MagicMock()
        mock_kafka_produce_handler.return_value = mock_handler_instance

        sut = KafkaBatchSender(topic="test_topic", transactional_id="test_transactional_id")

        self.assertEqual("test_topic", sut.topic)
        self.assertEqual([], sut.latest_messages)
        self.assertEqual([], sut.earlier_messages)
        self.assertEqual(False, sut.buffer)
        self.assertIsNone(sut.timer)
        self.assertEqual(mock_handler_instance, sut.kafka_produce_handler)
        self.assertEqual(mock_lock_instance, sut.lock)

        mock_lock.assert_called_once()
        mock_kafka_produce_handler.assert_called_once_with(transactional_id="test_transactional_id")

    @patch('heidgaf_core.batch_handler.KafkaProduceHandler')
    @patch('heidgaf_core.batch_handler.Lock')
    def test_init_with_puffer(self, mock_lock, mock_kafka_produce_handler):
        mock_lock_instance = MagicMock()
        mock_lock.return_value = mock_lock_instance
        mock_handler_instance = MagicMock()
        mock_kafka_produce_handler.return_value = mock_handler_instance

        sut = KafkaBatchSender(topic="test_topic", transactional_id="test_transactional_id", buffer=True)

        self.assertEqual("test_topic", sut.topic)
        self.assertEqual([], sut.latest_messages)
        self.assertEqual([], sut.earlier_messages)
        self.assertEqual(True, sut.buffer)
        self.assertIsNone(sut.timer)
        self.assertEqual(mock_handler_instance, sut.kafka_produce_handler)
        self.assertEqual(mock_lock_instance, sut.lock)

        mock_lock.assert_called_once()
        mock_kafka_produce_handler.assert_called_once_with(transactional_id="test_transactional_id")


class TestAddMessage(unittest.TestCase):
    @patch('heidgaf_core.batch_handler.KafkaProduceHandler')
    @patch('heidgaf_core.batch_handler.KafkaBatchSender._send_batch')
    @patch('heidgaf_core.batch_handler.KafkaBatchSender._reset_timer')
    def test_add_message_normal(self, mock_send_batch, mock_reset_timer, mock_produce_handler):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = KafkaBatchSender(topic="test_topic", transactional_id="test_transactional_id")
        sut.timer = MagicMock()
        sut.add_message("Message")

        mock_send_batch.assert_not_called()
        mock_reset_timer.assert_not_called()

    @patch('heidgaf_core.batch_handler.KafkaProduceHandler')
    @patch('heidgaf_core.batch_handler.KafkaBatchSender._send_batch')
    def test_add_message_full_messages(self, mock_send_batch, mock_produce_handler):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = KafkaBatchSender(topic="test_topic", transactional_id="test_transactional_id")
        sut.timer = MagicMock()

        for i in range(BATCH_SIZE - 1):
            sut.add_message(f"Message {i}")

        mock_send_batch.assert_not_called()
        sut.add_message(f"Message {BATCH_SIZE}")
        mock_send_batch.assert_called_once()

    @patch('heidgaf_core.batch_handler.KafkaProduceHandler')
    @patch('heidgaf_core.batch_handler.KafkaBatchSender._reset_timer')
    def test_add_message_no_timer(self, mock_reset_timer, mock_produce_handler):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sender_instance = KafkaBatchSender(topic="test_topic", transactional_id="test_transactional_id")
        sender_instance.timer = None

        sender_instance.add_message("Message")
        mock_reset_timer.assert_called_once()


class TestClose(unittest.TestCase):
    @patch('heidgaf_core.batch_handler.KafkaProduceHandler')
    @patch('heidgaf_core.batch_handler.Timer')
    def test_close_with_active_timer(self, mock_timer, mock_produce_handler):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = KafkaBatchSender(topic="test_topic", transactional_id="test_transactional_id")
        sut.timer = mock_timer
        sut._send_batch = MagicMock()
        sut.close()

        sut.timer.cancel.assert_called_once()
        sut._send_batch.assert_called_once()

    @patch('heidgaf_core.batch_handler.KafkaProduceHandler')
    def test_close_without_timer(self, mock_produce_handler):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sender_instance = KafkaBatchSender(topic="test_topic", transactional_id="test_transactional_id")
        sender_instance._send_batch = MagicMock()
        sender_instance.close()

        sender_instance._send_batch.assert_called_once()


class TestSendBatch(unittest.TestCase):
    @patch('heidgaf_core.batch_handler.KafkaProduceHandler')
    def test_send_batch_with_messages_without_puffer(self, mock_produce_handler):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = KafkaBatchSender(topic="test_topic", transactional_id="test_transactional_id")
        sut._reset_timer = MagicMock()
        sut.latest_messages = ["message1", "message2"]
        sut._send_batch()

        self.assertEqual(sut.latest_messages, [])
        self.assertEqual(sut.earlier_messages, [])
        mock_produce_handler_instance.send.assert_called_once_with(
            topic="test_topic",
            data=json.dumps(["message1", "message2"])
        )

        sut._reset_timer.assert_called_once()

    @patch('heidgaf_core.batch_handler.KafkaProduceHandler')
    def test_send_batch_without_messages_without_puffer(self, mock_produce_handler):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = KafkaBatchSender(topic="test_topic", transactional_id="test_transactional_id")
        sut._reset_timer = MagicMock()
        sut.latest_messages = []
        sut._send_batch()

        self.assertEqual(sut.latest_messages, [])
        self.assertEqual(sut.earlier_messages, [])

        mock_produce_handler_instance.send.assert_not_called()
        sut._reset_timer.assert_called_once()

    @patch('heidgaf_core.batch_handler.KafkaProduceHandler')
    def test_send_batch_with_messages_with_empty_puffer(self, mock_produce_handler):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = KafkaBatchSender(topic="test_topic", transactional_id="test_transactional_id", buffer=True)
        sut._reset_timer = MagicMock()
        sut.earlier_messages = []
        sut.latest_messages = ["message1", "message2"]
        sut._send_batch()

        self.assertEqual([], sut.latest_messages)
        self.assertEqual(["message1", "message2"], sut.earlier_messages)
        mock_produce_handler_instance.send.assert_called_once_with(
            topic="test_topic",
            data=json.dumps(["message1", "message2"])
        )

        sut._reset_timer.assert_called_once()

    @patch('heidgaf_core.batch_handler.KafkaProduceHandler')
    def test_send_batch_without_messages_with_empty_puffer(self, mock_produce_handler):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = KafkaBatchSender(topic="test_topic", transactional_id="test_transactional_id", buffer=True)
        sut._reset_timer = MagicMock()
        sut.earlier_messages = []
        sut.latest_messages = []
        sut._send_batch()

        self.assertEqual([], sut.latest_messages)
        self.assertEqual([], sut.earlier_messages)

        mock_produce_handler_instance.send.assert_not_called()
        sut._reset_timer.assert_called_once()

    @patch('heidgaf_core.batch_handler.KafkaProduceHandler')
    def test_send_batch_with_messages_with_full_puffer(self, mock_produce_handler):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = KafkaBatchSender(topic="test_topic", transactional_id="test_transactional_id", buffer=True)
        sut._reset_timer = MagicMock()
        sut.earlier_messages = ["message1", "message2"]
        sut.latest_messages = ["message3", "message4"]
        sut._send_batch()

        self.assertEqual([], sut.latest_messages)
        self.assertEqual(["message3", "message4"], sut.earlier_messages)
        mock_produce_handler_instance.send.assert_called_once_with(
            topic="test_topic",
            data=json.dumps(["message1", "message2", "message3", "message4"])
        )

        sut._reset_timer.assert_called_once()

    @patch('heidgaf_core.batch_handler.KafkaProduceHandler')
    def test_send_batch_without_messages_with_full_puffer(self, mock_produce_handler):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = KafkaBatchSender(topic="test_topic", transactional_id="test_transactional_id", buffer=True)
        sut._reset_timer = MagicMock()
        sut.earlier_messages = ["message1", "message2"]
        sut.latest_messages = []
        sut._send_batch()

        self.assertEqual([], sut.latest_messages)
        self.assertEqual([], sut.earlier_messages)

        mock_produce_handler_instance.send.assert_called_once_with(
            topic="test_topic",
            data=json.dumps(["message1", "message2"])
        )
        sut._reset_timer.assert_called_once()


class TestResetTimer(unittest.TestCase):
    @patch('heidgaf_core.batch_handler.KafkaProduceHandler')
    @patch('heidgaf_core.batch_handler.Timer')
    def test_reset_timer_with_existing_timer(self, mock_timer, mock_produce_handler):
        mock_timer_instance = MagicMock()
        mock_timer.return_value = mock_timer_instance
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = KafkaBatchSender(topic="test_topic", transactional_id="test_transactional_id")
        sut.timer = mock_timer_instance
        sut._send_batch = MagicMock()
        sut._reset_timer()

        self.assertIsNotNone(sut.timer)

        mock_timer_instance.cancel.assert_called_once()
        mock_timer.assert_called_once_with(BATCH_TIMEOUT, sut._send_batch)
        sut.timer.start.assert_called_once()

    @patch('heidgaf_core.batch_handler.KafkaProduceHandler')
    @patch('heidgaf_core.batch_handler.Timer')
    def test_reset_timer_without_existing_timer(self, mock_timer, mock_produce_handler):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = KafkaBatchSender(topic="test_topic", transactional_id="test_transactional_id")
        sut._send_batch = MagicMock()
        sut._reset_timer()

        self.assertIsNotNone(sut.timer)

        mock_timer.assert_called_once_with(BATCH_TIMEOUT, sut._send_batch)
        sut.timer.start.assert_called_once()


if __name__ == '__main__':
    unittest.main()
