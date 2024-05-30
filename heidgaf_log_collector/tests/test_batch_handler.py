import unittest
from threading import Lock
from unittest.mock import patch

from heidgaf_log_collector.batch_handler import KafkaBatchSender

# placeholders
KAFKA_BROKER_HOST = "localhost"
KAFKA_BROKER_PORT = 9092


class TestInit(unittest.TestCase):
    @patch('heidgaf_log_collector.batch_handler.Producer')
    def test_init(self, mock_producer):
        mock_producer_instance = mock_producer.return_value

        test_topic = 'test_topic'

        sender = KafkaBatchSender(test_topic)

        mock_producer.assert_called_once_with({'bootstrap.servers': f"{KAFKA_BROKER_HOST}:{KAFKA_BROKER_PORT}"})

        self.assertEqual(sender.topic, test_topic)
        self.assertEqual(sender.messages, [])
        self.assertIsInstance(sender.lock, type(Lock()))
        self.assertIsNone(sender.timer)

        self.assertEqual(sender.kafka_producer, mock_producer_instance)


# TODO: Add the rest of the tests

if __name__ == '__main__':
    unittest.main()
