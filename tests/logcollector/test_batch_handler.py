import datetime
import json
import unittest
import uuid
from unittest.mock import patch, MagicMock

from src.logcollector.batch_handler import BufferedBatchSender


class TestInit(unittest.TestCase):
    @patch("src.logcollector.batch_handler.BufferedBatch")
    @patch("src.logcollector.batch_handler.ExactlyOnceKafkaProduceHandler")
    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_init_with_buffer(
        self, mock_clickhouse, mock_kafka_produce_handler, mock_buffered_batch
    ):
        # Arrange
        mock_handler_instance = MagicMock()
        mock_kafka_produce_handler.return_value = mock_handler_instance
        mock_batch_instance = MagicMock()
        mock_buffered_batch.return_value = mock_batch_instance

        # Act
        sut = BufferedBatchSender(collector_name="test-collector", produce_topics=["test_topic"])

        # Assert
        self.assertEqual(["test_topic"], sut.topics)
        self.assertEqual(mock_batch_instance, sut.batch)
        self.assertIsNone(sut.timer)
        self.assertEqual(mock_handler_instance, sut.kafka_produce_handler)

        mock_buffered_batch.assert_called_once()
        mock_kafka_produce_handler.assert_called_once()


class TestDel(unittest.TestCase):
    # TODO
    pass


class TestAddMessage(unittest.TestCase):
    @patch("src.logcollector.batch_handler.logger")
    @patch("src.logcollector.batch_handler.ExactlyOnceKafkaProduceHandler")
    @patch("src.logcollector.batch_handler.BufferedBatchSender._reset_timer")
    @patch("src.logcollector.batch_handler.BufferedBatchSender._send_batch_for_key")
    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_add_message_normal(
        self,
        mock_clickhouse,
        mock_send_batch,
        mock_reset_timer,
        mock_produce_handler,
        mock_logger,
    ):
        # Arrange
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        key = "test_key"
        message = json.dumps(
            dict(
                logline_id=str(uuid.uuid4()),
                data=f"test_message",
            )
        )

        sut = BufferedBatchSender(collector_name="test-collector",produce_topics=["test_topic"])

        sut.timer = MagicMock()

        # Act
        sut.add_message(key, message)

        # Assert
        mock_send_batch.assert_not_called()
        mock_reset_timer.assert_not_called()
        
    @patch("src.logcollector.batch_handler.get_batch_configuration")
    @patch("src.logcollector.batch_handler.logger")
    @patch("src.logcollector.batch_handler.ExactlyOnceKafkaProduceHandler")
    @patch("src.logcollector.batch_handler.BufferedBatchSender._send_batch_for_key")
    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_add_message_full_messages(
        self, mock_clickhouse, mock_send_batch, mock_produce_handler, mock_logger, mock_get_batch_config
    ):
        # Arrange
        mock_get_batch_config.return_value =  {
            "batch_size": 100,
            "batch_timeout": 5.9,
            "subnet_id": {
                "ipv4_prefix_length": "16",
                "ipv6_prefix_length": "32"
            }    
        }
        
        # Arrange
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        key = "test_key"

        sut = BufferedBatchSender(collector_name="test-collector",produce_topics=["test_topic"])

        sut.timer = MagicMock()

        # Act
        for i in range(99):
            test_message = json.dumps(
                dict(
                    logline_id=str(uuid.uuid4()),
                    data=f"message_{i}",
                )
            )
            sut.add_message(key, test_message)

        # Assert
        mock_send_batch.assert_not_called()
        sut.add_message(
            key,
            json.dumps(
                dict(
                    logline_id=str(uuid.uuid4()),
                    data="message_100",
                )
            ),
        )
        mock_send_batch.assert_called_once()
        
    @patch("src.logcollector.batch_handler.get_batch_configuration")
    @patch("src.logcollector.batch_handler.logger")
    @patch("src.logcollector.batch_handler.ExactlyOnceKafkaProduceHandler")
    @patch("src.logcollector.batch_handler.BufferedBatchSender._send_batch_for_key")
    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_add_message_full_messages_with_different_keys(
        self, mock_clickhouse, mock_send_batch, mock_produce_handler, mock_logger, mock_get_batch_config
    ):
        mock_get_batch_config.return_value =  {
            "batch_size": 100,
            "batch_timeout": 5.9,
            "subnet_id": {
                "ipv4_prefix_length": "16",
                "ipv6_prefix_length": "32"
            }
        }    
        
        # Arrange
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        key = "test_key"
        other_key = "other_key"

        sut = BufferedBatchSender(collector_name="test-collector",produce_topics=["test_topic"])

        sut.timer = MagicMock()

        # Act
        for i in range(79):
            sut.add_message(
                key,
                json.dumps(
                    dict(
                        logline_id=str(uuid.uuid4()),
                        data=f"message_{i}",
                    )
                ),
            )
        for i in range(15):
            sut.add_message(
                other_key,
                json.dumps(
                    dict(
                        logline_id=str(uuid.uuid4()),
                        data=f"message_{i}",
                    )
                ),
            )
        for i in range(20):
            sut.add_message(
                key,
                json.dumps(
                    dict(
                        logline_id=str(uuid.uuid4()),
                        data=f"message_{i}",
                    )
                ),
            )

        # Assert
        mock_send_batch.assert_not_called()
        sut.add_message(
            key,
            json.dumps(
                dict(
                    logline_id=str(uuid.uuid4()),
                    data="message_100",
                )
            ),
        )
        mock_send_batch.assert_called_once()

    @patch("src.logcollector.batch_handler.logger")
    @patch("src.logcollector.batch_handler.ExactlyOnceKafkaProduceHandler")
    @patch("src.logcollector.batch_handler.BufferedBatchSender._reset_timer")
    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_add_message_no_timer(
        self, mock_clickhouse, mock_reset_timer, mock_produce_handler, mock_logger
    ):
        # Arrange
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = BufferedBatchSender(collector_name="test-collector",produce_topics=["test_topic"])

        sut.timer = None

        # Act
        sut.add_message(
            "test_key",
            json.dumps(
                dict(
                    logline_id=str(uuid.uuid4()),
                    data="test_message",
                )
            ),
        )

        # Assert
        mock_reset_timer.assert_called_once()


class TestSendAllBatches(unittest.TestCase):
    @patch("src.logcollector.batch_handler.logger")
    @patch("src.logcollector.batch_handler.ExactlyOnceKafkaProduceHandler")
    @patch("src.logcollector.batch_handler.BufferedBatchSender._send_batch_for_key")
    @patch("src.logcollector.batch_handler.BufferedBatch")
    def test_send_all_batches_with_existing_keys(
        self,
        mock_buffered_batch,
        mock_send_batch,
        mock_kafka_produce_handler,
        mock_logger,
    ):
        # Arrange
        mock_batch_instance = MagicMock()
        mock_buffered_batch.return_value = mock_batch_instance
        mock_batch_instance.get_stored_keys.return_value = ["key_1", "key_2"]
        mock_send_batch_instance = MagicMock()
        mock_send_batch.return_value = mock_send_batch_instance

        sut = BufferedBatchSender(collector_name="test-collector",produce_topics=["test_topic"])


        # Act
        sut._send_all_batches(reset_timer=False)

        # Assert
        mock_send_batch.assert_any_call("key_1")
        mock_send_batch.assert_any_call("key_2")
        self.assertEqual(mock_send_batch.call_count, 2)

    @patch("src.logcollector.batch_handler.ExactlyOnceKafkaProduceHandler")
    @patch("src.logcollector.batch_handler.BufferedBatchSender._send_batch_for_key")
    @patch("src.logcollector.batch_handler.BufferedBatch")
    def test_send_all_batches_with_one_key(
        self, mock_buffered_batch, mock_send_batch, mock_kafka_produce_handler
    ):
        # Arrange
        mock_batch_instance = MagicMock()
        mock_buffered_batch.return_value = mock_batch_instance
        mock_batch_instance.get_stored_keys.return_value = []
        mock_send_batch_instance = MagicMock()
        mock_send_batch.return_value = mock_send_batch_instance

        sut = BufferedBatchSender(collector_name="test-collector",produce_topics=["test_topic"])


        # Act
        sut._send_all_batches(reset_timer=False)

        # Assert
        self.assertEqual(mock_send_batch.call_count, 0)

    @patch("src.logcollector.batch_handler.logger")
    @patch("src.logcollector.batch_handler.ExactlyOnceKafkaProduceHandler")
    @patch("src.logcollector.batch_handler.BufferedBatchSender._send_batch_for_key")
    @patch("src.logcollector.batch_handler.BufferedBatchSender._reset_timer")
    @patch("src.logcollector.batch_handler.BufferedBatch")
    def test_send_all_batches_with_existing_keys_and_reset_timer(
        self,
        mock_buffered_batch,
        mock_reset_timer,
        mock_send_batch,
        mock_kafka_produce_handler,
        mock_logger,
    ):
        # Arrange
        mock_batch_instance = MagicMock()
        mock_buffered_batch.return_value = mock_batch_instance
        mock_batch_instance.get_stored_keys.return_value = ["key_1", "key_2"]
        mock_send_batch_instance = MagicMock()
        mock_send_batch.return_value = mock_send_batch_instance

        sut = BufferedBatchSender(collector_name="test-collector",produce_topics=["test_topic"])


        # Act
        sut._send_all_batches(reset_timer=True)

        # Assert
        mock_send_batch.assert_any_call("key_1")
        mock_send_batch.assert_any_call("key_2")
        mock_reset_timer.assert_called_once()
        self.assertEqual(mock_send_batch.call_count, 2)

    @patch("src.logcollector.batch_handler.ExactlyOnceKafkaProduceHandler")
    @patch("src.logcollector.batch_handler.BufferedBatchSender._send_batch_for_key")
    @patch("src.logcollector.batch_handler.BufferedBatch")
    def test_send_all_batches_with_no_keys(
        self, mock_buffered_batch, mock_send_batch, mock_kafka_produce_handler
    ):
        # Arrange
        mock_batch_instance = MagicMock()
        mock_buffered_batch.return_value = mock_batch_instance
        mock_batch_instance.get_stored_keys.return_value = []
        mock_send_batch_instance = MagicMock()
        mock_send_batch.return_value = mock_send_batch_instance

        sut = BufferedBatchSender(collector_name="test-collector",produce_topics=["test_topic"])


        # Act
        sut._send_all_batches(reset_timer=False)

        # Assert
        mock_send_batch.assert_not_called()


class TestSendBatchForKey(unittest.TestCase):
    @patch("src.logcollector.batch_handler.ExactlyOnceKafkaProduceHandler")
    @patch.object(BufferedBatchSender, "_send_data_packet")
    @patch("src.logcollector.batch_handler.BufferedBatch")
    def test_send_batch_for_key_success(
        self, mock_batch, mock_send_data_packet, mock_produce_handler
    ):
        # Arrange
        mock_batch_instance = MagicMock()
        mock_batch.return_value = mock_batch_instance
        mock_batch_instance.complete_batch.return_value = "mock_data_packet"

        sut = BufferedBatchSender(collector_name="test-collector",produce_topics=["test_topic"])

        key = "test_key"

        # Act
        sut._send_batch_for_key(key)

        # Assert
        mock_batch_instance.complete_batch.assert_called_once_with(key)
        mock_send_data_packet.assert_called_once_with(key, "mock_data_packet")

    @patch("src.logcollector.batch_handler.ExactlyOnceKafkaProduceHandler")
    @patch.object(BufferedBatchSender, "_send_data_packet")
    @patch("src.logcollector.batch_handler.BufferedBatch")
    def test_send_batch_for_key_value_error(
        self, mock_batch, mock_send_data_packet, mock_produce_handler
    ):
        # Arrange
        mock_batch_instance = MagicMock()
        mock_batch.return_value = mock_batch_instance
        mock_batch_instance.complete_batch.side_effect = ValueError("Mock exception")

        sut = BufferedBatchSender(collector_name="test-collector",produce_topics=["test_topic"])

        key = "test_key"

        # Act
        sut._send_batch_for_key(key)

        # Assert
        mock_batch_instance.complete_batch.assert_called_once_with(key)
        mock_send_data_packet.assert_not_called()


class TestSendDataPacket(unittest.TestCase):
    @patch("src.logcollector.batch_handler.ExactlyOnceKafkaProduceHandler")
    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_send_data_packet(self, mock_clickhouse, mock_produce_handler):
        # Arrange
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance
        mock_produce_handler_instance.send.return_value = None

        key = "test_key"
        data = {
            "batch_id": uuid.UUID("b4b6f13e-d064-4ab7-94ed-d02b46063308"),
            "begin_timestamp": datetime.datetime(2024, 12, 6, 13, 12, 30, 324015),
            "end_timestamp": datetime.datetime(2024, 12, 6, 13, 12, 31, 832173),
            "data": ["test_data"],
        }

        sut = BufferedBatchSender(collector_name="test-collector",produce_topics=["test_topic"])

        # Act
        sut._send_data_packet(key, data)

        # Assert
        mock_produce_handler_instance.produce.assert_called_once_with(
            topic="test_topic",
            data='{"batch_id": "b4b6f13e-d064-4ab7-94ed-d02b46063308", "begin_timestamp": '
            '"2024-12-06T13:12:30.324015", "end_timestamp": "2024-12-06T13:12:31.832173", '
            '"data": ["test_data"]}',
            key=key,
        )


class TestResetTimer(unittest.TestCase):
    @patch("src.logcollector.batch_handler.get_batch_configuration")
    @patch("src.logcollector.batch_handler.ExactlyOnceKafkaProduceHandler")
    @patch("src.logcollector.batch_handler.Timer")
    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_reset_timer_with_existing_timer(
        self, mock_clickhouse, mock_timer, mock_produce_handler, mock_get_batch_config
    ):
        # Arrange
        mock_get_batch_config.return_value =  {
            "batch_size": "200000",
            "batch_timeout": 5.9,
            "subnet_id": {
                "ipv4_prefix_length": "16",
                "ipv6_prefix_length": "32"
            }    
        }
        mock_timer_instance = MagicMock()
        mock_timer.return_value = mock_timer_instance
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = BufferedBatchSender(collector_name="test-collector",produce_topics=["test_topic"])

        sut.timer = mock_timer_instance
        sut._send_all_batches = MagicMock()

        # Act
        sut._reset_timer()

        # Assert
        self.assertIsNotNone(sut.timer)

        mock_timer_instance.cancel.assert_called_once()
        mock_timer.assert_called_once_with(5.9, sut._send_all_batches)
        sut.timer.start.assert_called_once()

    @patch("src.logcollector.batch_handler.get_batch_configuration")
    @patch("src.logcollector.batch_handler.ExactlyOnceKafkaProduceHandler")
    @patch("src.logcollector.batch_handler.Timer")
    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_reset_timer_without_existing_timer(
        self, mock_clickhouse, mock_timer, mock_produce_handler, mock_get_batch_config
    ):
        
        mock_get_batch_config.return_value =  {
            "batch_size": "200000",
            "batch_timeout": 4.6,
            "subnet_id": {
                "ipv4_prefix_length": "16",
                "ipv6_prefix_length": "32"
            }         
        }
        # Arrange
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = BufferedBatchSender(collector_name="test-collector",produce_topics=["test_topic"])

        sut._send_all_batches = MagicMock()

        # Act
        sut._reset_timer()

        # Assert
        self.assertIsNotNone(sut.timer)

        mock_timer.assert_called_once_with(4.6, sut._send_all_batches)
        sut.timer.start.assert_called_once()


if __name__ == "__main__":
    unittest.main()
