import unittest
from unittest.mock import MagicMock, patch

from confluent_kafka import KafkaException

from src.base.kafka_handler import KafkaConsumeHandler

CONSUMER_GROUP_ID = "test_group_id"


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
    def test_init(self, mock_consumer):
        mock_consumer_instance = MagicMock()
        mock_consumer.return_value = mock_consumer_instance

        expected_conf = {
            "bootstrap.servers": "127.0.0.1:9999,127.0.0.2:9998,127.0.0.3:9997",
            "group.id": CONSUMER_GROUP_ID,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
        }

        sut = KafkaConsumeHandler(topic="test_topic")

        self.assertEqual(mock_consumer_instance, sut.consumer)

        mock_consumer.assert_called_once_with(expected_conf)
        mock_consumer_instance.assign.assert_called_once()

    @patch("src.base.kafka_handler.logger")
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
    def test_init_fail(self, mock_consumer, mock_logger):
        mock_consumer_instance = MagicMock()
        mock_consumer.return_value = mock_consumer_instance

        expected_conf = {
            "bootstrap.servers": "127.0.0.1:9999,127.0.0.2:9998,127.0.0.3:9997",
            "group.id": CONSUMER_GROUP_ID,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
        }

        with patch.object(mock_consumer_instance, "assign", side_effect=KafkaException):
            with self.assertRaises(KafkaException):
                sut = KafkaConsumeHandler(topic="test_topic")

                self.assertEqual(mock_consumer_instance, sut.consumer)

                mock_consumer.assert_called_once_with(expected_conf)
                mock_consumer_instance.assign.assert_called_once()


class TestDel(unittest.TestCase):
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
    def test_del_with_existing_consumer(self, mock_consumer):
        # Arrange
        mock_consumer_instance = MagicMock()
        mock_consumer.return_value = mock_consumer_instance

        sut = KafkaConsumeHandler(topic="test_topic")
        sut.consumer = mock_consumer_instance

        # Act
        del sut

        # Assert
        mock_consumer_instance.close.assert_called_once()

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
    def test_del_with_existing_consumer(self, mock_consumer):
        # Arrange
        mock_consumer_instance = MagicMock()
        mock_consumer.return_value = mock_consumer_instance

        sut = KafkaConsumeHandler(topic="test_topic")
        sut.consumer = None

        # Act
        del sut

        # Assert
        mock_consumer_instance.close.assert_not_called()


if __name__ == "__main__":
    unittest.main()
