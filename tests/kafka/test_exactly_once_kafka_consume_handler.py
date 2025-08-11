import datetime
import json
import unittest
import uuid
from unittest.mock import patch, Mock

import marshmallow_dataclass
from confluent_kafka import KafkaException, KafkaError

from src.base.data_classes.batch import Batch
from src.base.kafka_handler import ExactlyOnceKafkaConsumeHandler

CONSUMER_GROUP_ID = "default_gid"


class TestInit(unittest.TestCase):
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
    def test_init(self, mock_consumer, mock_admin_client, mock_all_topics_created):
        mock_consumer_instance = Mock()
        mock_consumer.return_value = mock_consumer_instance

        expected_conf = {
            "bootstrap.servers": "127.0.0.1:9999,127.0.0.2:9998,127.0.0.3:9997",
            "group.id": CONSUMER_GROUP_ID,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
        }

        sut = ExactlyOnceKafkaConsumeHandler(topics="test_topic")

        self.assertEqual(mock_consumer_instance, sut.consumer)

        mock_consumer.assert_called_once_with(expected_conf)
        mock_consumer_instance.subscribe.assert_called_once()

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
    def test_init_fail(self, mock_consumer, mock_admin_client, mock_all_topics_created):
        mock_consumer_instance = Mock()
        mock_consumer.return_value = mock_consumer_instance

        expected_conf = {
            "bootstrap.servers": "127.0.0.1:9999,127.0.0.2:9998,127.0.0.3:9997",
            "group.id": CONSUMER_GROUP_ID,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
        }

        with patch.object(
            mock_consumer_instance, "subscribe", side_effect=KafkaException
        ):
            with self.assertRaises(KafkaException):
                sut = ExactlyOnceKafkaConsumeHandler(topics="test_topic")

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
    @patch(
        "src.base.kafka_handler.KafkaConsumeHandler._all_topics_created",
        return_value=True,
    )
    @patch("src.base.kafka_handler.AdminClient")
    def setUp(self, mock_admin_client, mock_all_topics_created, mock_consumer):
        self.mock_consumer = mock_consumer
        self.topics = ["test_topic_1", "test_topic_2"]
        self.sut = ExactlyOnceKafkaConsumeHandler(self.topics)

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
        self.sut.consumer.commit = Mock()

        result = None
        try:
            result = self.sut.consume()
        except StopIteration:
            pass

        self.sut.consumer.commit.assert_called_once()
        self.assertEqual((key, value, topic), result)

    def test_consumer_raises_keyboard_interrupt(self):
        self.sut.consumer.poll.side_effect = [KeyboardInterrupt]

        self.sut.consume()

        self.assertTrue(True)


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
    @patch(
        "src.base.kafka_handler.KafkaConsumeHandler._all_topics_created",
        return_value=True,
    )
    @patch("src.base.kafka_handler.AdminClient")
    def test_del_with_existing_consumer(
        self, mock_admin_client, mock_all_topics_created, mock_consumer
    ):
        # Arrange
        mock_consumer_instance = Mock()
        mock_consumer.return_value = mock_consumer_instance

        sut = ExactlyOnceKafkaConsumeHandler(topics="test_topic")
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
    @patch(
        "src.base.kafka_handler.KafkaConsumeHandler._all_topics_created",
        return_value=True,
    )
    @patch("src.base.kafka_handler.AdminClient")
    def test_del_with_existing_consumer(
        self, mock_admin_client, mock_all_topics_created, mock_consumer
    ):
        # Arrange
        mock_consumer_instance = Mock()
        mock_consumer.return_value = mock_consumer_instance

        sut = ExactlyOnceKafkaConsumeHandler(topics="test_topic")
        sut.consumer = None

        # Act
        del sut

        # Assert
        mock_consumer_instance.close.assert_not_called()


class TestDict(unittest.TestCase):
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
    @patch(
        "src.base.kafka_handler.KafkaConsumeHandler._all_topics_created",
        return_value=True,
    )
    @patch("src.base.kafka_handler.AdminClient")
    def test_dict(self, mock_admin_client, mock_all_topics_created, mock_consumer):
        mock_consumer_instance = Mock()
        mock_consumer.return_value = mock_consumer_instance

        sut = ExactlyOnceKafkaConsumeHandler(topics="test_topic")
        self.assertTrue(sut._is_dicts([{}, {}]))


class TestConsumeAsObject(unittest.TestCase):
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
        self.sut = ExactlyOnceKafkaConsumeHandler(topics="test_topic")

    def test_consume_as_object_no_key_no_value(self):
        with patch(
            "src.base.kafka_handler.ExactlyOnceKafkaConsumeHandler.consume"
        ) as mock_consume:
            mock_consume.return_value = [None, None, None]

            result = self.sut.consume_as_object()

        self.assertEqual(result, (None, {}))

    def test_consume_as_object_valid_data(self):
        key = "valid_key"
        batch_schema = marshmallow_dataclass.class_schema(Batch)()
        value = batch_schema.dumps(
            {
                "batch_id": uuid.uuid4(),
                "batch_tree_row_id": uuid.uuid4(),
                "begin_timestamp": datetime.datetime.now(),
                "end_timestamp": datetime.datetime.now(),
                "data": [{"field1": "value1", "field2": "value2"}],
            }
        )
        topic = "test_topic"

        with patch(
            "src.base.kafka_handler.ExactlyOnceKafkaConsumeHandler.consume"
        ) as mock_consume:
            mock_consume.return_value = [key, value, topic]

            result = self.sut.consume_as_object()

        self.assertEqual(result[0], key)
        self.assertIsInstance(result[1], Batch)

    def test_consume_as_object_valid_data_with_inner_strings(self):
        key = "valid_key"
        batch_schema = marshmallow_dataclass.class_schema(Batch)()
        value = batch_schema.dumps(
            {
                "batch_id": uuid.uuid4(),
                "batch_tree_row_id": uuid.uuid4(),
                "begin_timestamp": datetime.datetime.now(),
                "end_timestamp": datetime.datetime.now(),
                "data": [
                    '{"field1": "value1", "field2": "value2"}',
                    '{"field3": "value3", "field4": "value4"}',
                ],
            }
        )
        topic = "test_topic"

        with patch(
            "src.base.kafka_handler.ExactlyOnceKafkaConsumeHandler.consume"
        ) as mock_consume:
            mock_consume.return_value = [key, value, topic]

            result = self.sut.consume_as_object()

        self.assertEqual(result[0], key)
        self.assertIsInstance(result[1], Batch)

    def test_consume_as_object_invalid_data(self):
        key = "invalid_key"
        value = json.dumps(
            {"data": {"field1": "value1", "field2": "value2"}}  # invalid format
        )
        topic = "test_topic"

        with patch(
            "src.base.kafka_handler.ExactlyOnceKafkaConsumeHandler.consume"
        ) as mock_consume:
            mock_consume.return_value = [key, value, topic]

            with self.assertRaises(ValueError):
                self.sut.consume_as_object()

    @patch("src.base.kafka_handler.marshmallow_dataclass.class_schema")
    def test_consume_as_object_invalid_batch(self, mock_schema):
        key = "valid_key"
        value = json.dumps({"data": [{"field1": "value1", "field2": "value2"}]})
        topic = "test_topic"

        mock_schema_instance = Mock()
        mock_schema.return_value = mock_schema_instance

        mock_schema_instance.load.return_value = None

        with patch(
            "src.base.kafka_handler.ExactlyOnceKafkaConsumeHandler.consume"
        ) as mock_consume:
            mock_consume.return_value = [key, value, topic]

            with self.assertRaises(ValueError):
                self.sut.consume_as_object()


if __name__ == "__main__":
    unittest.main()
