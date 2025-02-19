import json
import unittest
from unittest.mock import patch, MagicMock

from src.base.kafka_handler import (
    KafkaConsumeHandler,
    KafkaMessageFetchException,
    TooManyFailedAttemptsError,
)


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
        mock_consumer_instance.subscribe.assert_called_once()

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
        return_value=False,
    )
    @patch("src.base.kafka_handler.AdminClient")
    @patch("src.base.kafka_handler.Consumer")
    def test_init_unsuccessful(
        self, mock_consumer, mock_admin_client, mock_all_topics_created
    ):
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
        with self.assertRaises(TooManyFailedAttemptsError):
            KafkaConsumeHandler(topics="test_topic")

        # Assert
        mock_consumer.assert_called_once_with(expected_conf)
        mock_consumer_instance.subscribe.assert_not_called()

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
    def test_init_successful_with_list(
        self, mock_consumer, mock_admin_client, mock_all_topics_created
    ):
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
    def test_not_implemented(
        self, mock_consumer, mock_admin_client, mock_all_topics_created
    ):
        # Arrange
        sut = KafkaConsumeHandler(topics="test_topic")

        # Act and Assert
        with self.assertRaises(NotImplementedError):
            sut.consume()


class TestConsumeAsJSON(unittest.TestCase):
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
        self.sut = KafkaConsumeHandler(topics="test_topic")

    def test_successful(self):
        with patch(
            "src.base.kafka_handler.KafkaConsumeHandler.consume"
        ) as mock_consume:
            # Arrange
            mock_consume.return_value = [
                "test_key",
                json.dumps(dict(test_value=123)),
                "test_topic",
            ]

            # Act
            returned_values = self.sut.consume_as_json()

        # Assert
        self.assertEqual(("test_key", dict(test_value=123)), returned_values)

    def test_wrong_data_format(self):
        with patch(
            "src.base.kafka_handler.KafkaConsumeHandler.consume"
        ) as mock_consume:
            # Arrange
            mock_consume.return_value = ["test_key", "wrong_format", "test_topic"]

            # Act and Assert
            with self.assertRaises(ValueError):
                self.sut.consume_as_json()

    def test_wrong_data_format_list(self):
        with patch(
            "src.base.kafka_handler.KafkaConsumeHandler.consume"
        ) as mock_consume:
            # Arrange
            mock_consume.return_value = [
                "test_key",
                json.dumps([1, 2, 3]),
                "test_topic",
            ]

            # Act and Assert
            with self.assertRaises(ValueError):
                self.sut.consume_as_json()

    def test_kafka_message_fetch_exception(self):
        with patch(
            "src.base.kafka_handler.KafkaConsumeHandler.consume",
            side_effect=KafkaMessageFetchException,
        ):
            # Act and Assert
            with self.assertRaises(KafkaMessageFetchException):
                self.sut.consume_as_json()

    def test_keyboard_interrupt(self):
        with patch(
            "src.base.kafka_handler.KafkaConsumeHandler.consume",
            side_effect=KeyboardInterrupt,
        ):
            # Act and Assert
            with self.assertRaises(KeyboardInterrupt):
                self.sut.consume_as_json()

    def test_kafka_message_else(self):
        with patch(
            "src.base.kafka_handler.KafkaConsumeHandler.consume"
        ) as mock_consume:
            # Arrange
            mock_consume.return_value = [None, None, "test_topic"]

            # Act
            returned_values = self.sut.consume_as_json()

        # Assert
        self.assertEqual((None, {}), returned_values)


class TestAllTopicsCreated(unittest.TestCase):
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
        self.sut = KafkaConsumeHandler(topics=["test_topic", "another_topic"])

    @patch("src.base.kafka_handler.Consumer")
    def test_with_all_created(self, mock_consumer):
        # Arrange
        mock_topics = MagicMock()
        mock_topics.topics = ["test_topic", "another_topic"]

        self.sut.consumer = MagicMock()
        self.sut.consumer.list_topics.return_value = mock_topics

        # Act and Assert
        self.assertTrue(
            self.sut._all_topics_created(topics=["test_topic", "another_topic"])
        )

    @patch("src.base.kafka_handler.time.sleep")
    @patch("src.base.kafka_handler.Consumer")
    def test_with_none_created(self, mock_consumer, mock_sleep):
        # Arrange
        mock_topics = MagicMock()
        mock_topics.topics = []

        self.sut.consumer = MagicMock()
        self.sut.consumer.list_topics.return_value = mock_topics

        # Act and Assert
        self.assertFalse(
            self.sut._all_topics_created(topics=["test_topic", "another_topic"])
        )


if __name__ == "__main__":
    unittest.main()
