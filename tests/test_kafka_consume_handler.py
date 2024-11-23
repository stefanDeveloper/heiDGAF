import unittest
from unittest.mock import patch, MagicMock

from src.base.kafka_handler import KafkaConsumeHandler


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
    @patch("src.base.kafka_handler.Consumer")
    def test_init_successful(self, mock_consumer):
        # Arrange
        mock_consumer_instance = MagicMock()
        mock_consumer.return_value = mock_consumer_instance

        expected_conf = {
            "bootstrap.servers": "127.0.0.1:9999,127.0.0.2:9998,127.0.0.3:9997",
            "group.id": "test_group_id",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
        }

        # Act
        sut = KafkaConsumeHandler(topics="test_topic")

        # Assert
        self.assertEqual(mock_consumer_instance, sut.consumer)

        mock_consumer.assert_called_once_with(expected_conf)
        mock_consumer_instance.assign.assert_called_once()

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
    @patch("src.base.kafka_handler.Consumer")
    def test_init_successful_with_list(self, mock_consumer):
        # Arrange
        mock_consumer_instance = MagicMock()
        mock_consumer.return_value = mock_consumer_instance

        expected_conf = {
            "bootstrap.servers": "127.0.0.1:9999,127.0.0.2:9998,127.0.0.3:9997",
            "group.id": "test_group_id",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
        }

        # Act
        sut = KafkaConsumeHandler(topics=["test_topic_1", "test_topic_2"])

        # Assert
        self.assertEqual(mock_consumer_instance, sut.consumer)

        mock_consumer.assert_called_once_with(expected_conf)
        mock_consumer_instance.assign.assert_called_once()


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
    @patch("src.base.kafka_handler.Consumer")
    def test_not_implemented(self, mock_consumer):
        # Arrange
        sut = KafkaConsumeHandler(topics="test_topic")

        # Act and Assert
        with self.assertRaises(NotImplementedError):
            sut.consume()


if __name__ == "__main__":
    unittest.main()
