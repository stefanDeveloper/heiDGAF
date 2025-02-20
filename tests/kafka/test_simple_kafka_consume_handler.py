import unittest
from unittest.mock import patch, Mock

from confluent_kafka import KafkaError

from src.base.kafka_handler import SimpleKafkaConsumeHandler


class TestInit(unittest.TestCase):
    @patch("src.base.kafka_handler.CONSUMER_GROUP_ID", "test_group_id")
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
    @patch(
        "src.base.kafka_handler.KafkaConsumeHandler._all_topics_created",
        return_value=True,
    )
    @patch("src.base.kafka_handler.AdminClient")
    @patch("src.base.kafka_handler.Consumer")
    def test_init_successful(
        self, mock_consumer, mock_admin_client, mock_all_topics_created
    ):
        # Arrange
        mock_consumer_instance = Mock()
        mock_consumer.return_value = mock_consumer_instance

        expected_conf = {
            "bootstrap.servers": "127.0.0.1:9999,127.0.0.2:9998,127.0.0.3:9997",
            "group.id": "test_group_id",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
        }

        # Act
        sut = SimpleKafkaConsumeHandler(topics="test_topic")

        # Assert
        self.assertEqual(mock_consumer_instance, sut.consumer)

        mock_consumer.assert_called_once_with(expected_conf)
        mock_consumer_instance.subscribe.assert_called_once()


class TestConsume(unittest.TestCase):
    @patch("src.base.kafka_handler.CONSUMER_GROUP_ID", "test_group_id")
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
    @patch(
        "src.base.kafka_handler.KafkaConsumeHandler._all_topics_created",
        return_value=True,
    )
    @patch("src.base.kafka_handler.AdminClient")
    @patch("src.base.kafka_handler.Consumer")
    def setUp(self, mock_consumer, mock_admin_client, mock_all_topics_created):
        self.mock_consumer = mock_consumer
        self.topics = ["test_topic_1", "test_topic_2"]
        self.sut = SimpleKafkaConsumeHandler(self.topics)

    def test_no_messages_polling(self):
        self.sut.consumer.poll.side_effect = [None, None, None, StopIteration]

        result = None
        try:
            result = self.sut.consume()
        except StopIteration:
            pass

        self.assertIsNone(result)

    def test_consumer_error_partition_eof(self):
        eof_error = Mock()
        eof_error.code.return_value = KafkaError._PARTITION_EOF

        msg = Mock()
        msg.error.return_value = eof_error
        self.sut.consumer.poll.side_effect = [msg, StopIteration]

        result = None
        try:
            result = self.sut.consume()
        except StopIteration:
            pass

        self.assertIsNone(result)

    def test_consumer_raises_other_error(self):
        other_error = Mock()
        other_error.code.return_value = KafkaError._ALL_BROKERS_DOWN

        msg = Mock()
        msg.error.return_value = other_error

        self.sut.consumer.poll.side_effect = [msg]

        with self.assertRaises(Exception):
            self.sut.consume()

    def test_message_processing(self):
        key = "test_key"
        value = "test_value"
        topic = "test_topic"

        msg = Mock()
        msg.key.return_value = key.encode("utf-8")
        msg.value.return_value = value.encode("utf-8")
        msg.topic.return_value = topic
        msg.error.return_value = None

        self.sut.consumer.poll.side_effect = [msg, StopIteration]

        result = None
        try:
            result = self.sut.consume()
        except StopIteration:
            pass

        self.assertEqual((key, value, topic), result)

    def test_consumer_raises_keyboard_interrupt(self):
        self.sut.consumer.poll.side_effect = [KeyboardInterrupt]

        self.sut.consume()

        self.assertTrue(True)


if __name__ == "__main__":
    unittest.main()
