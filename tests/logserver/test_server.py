import asyncio
import os
import tempfile
import unittest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import aiofiles

from src.logserver.server import LogServer, main

LOG_SERVER_IP_ADDR = "192.168.0.1"


class TestInit(unittest.TestCase):
    @patch("src.logserver.server.ExactlyOnceKafkaProduceHandler")
    @patch("src.logserver.server.ExactlyOnceKafkaConsumeHandler")
    @patch("src.logserver.server.ClickHouseKafkaSender")
    def test_valid_init(
        self, mock_clickhouse, mock_kafka_consume_handler, mock_kafka_produce_handler
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_produce_handler_instance = MagicMock()

        mock_kafka_produce_handler.return_value = mock_kafka_produce_handler_instance
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = LogServer(consume_topic="test_topic", produce_topics=[])
        self.assertEqual(mock_kafka_consume_handler_instance, sut.kafka_consume_handler)
        self.assertEqual(mock_kafka_produce_handler_instance, sut.kafka_produce_handler)
        mock_kafka_consume_handler.assert_called_once_with("test_topic")


class TestStart(unittest.IsolatedAsyncioTestCase):
    @patch("src.logserver.server.logger")
    @patch("src.logserver.server.ExactlyOnceKafkaConsumeHandler")
    @patch("src.logserver.server.ExactlyOnceKafkaProduceHandler")
    @patch("src.logserver.server.ClickHouseKafkaSender")
    def setUp(
        self,
        mock_clickhouse,
        mock_kafka_produce_handler,
        mock_kafka_consume_handler,
        mock_logger,
    ):
        self.sut = LogServer(consume_topic="consume-topic", produce_topics=["topic1","topic2"])

    @patch("src.logserver.server.LogServer.fetch_from_kafka")
    @patch("src.logserver.server.ClickHouseKafkaSender")
    async def test_start(
        self,
        mock_clickhouse,
        mock_fetch_from_kafka,
    ):
        # Act
        await self.sut.start()

        # Assert
        mock_fetch_from_kafka.assert_called_once()

    # @patch("src.logserver.server.LogServer.fetch_from_kafka")
    # @patch("src.logserver.server.LogServer.fetch_from_file")
    # @patch("src.logserver.server.ClickHouseKafkaSender")
    # async def test_start_handles_keyboard_interrupt(
    #     self,
    #     mock_clickhouse,
    #     mock_fetch_from_file,
    #     mock_fetch_from_kafka,
    # ):
    #     # Arrange
    #     async def mock_gather(*args, **kwargs):
    #         raise KeyboardInterrupt

    #     with patch(
    #         "src.logserver.server.asyncio.gather", side_effect=mock_gather
    #     ) as mock:
    #         # Act
    #         await self.sut.start()

    #         # Assert
    #         mock.assert_called_once()
    #         mock_fetch_from_kafka.assert_called_once()
    #         mock_fetch_from_file.assert_called_once()


class TestSend(unittest.TestCase):
    @patch("src.logserver.server.ExactlyOnceKafkaProduceHandler")
    @patch("src.logserver.server.ExactlyOnceKafkaConsumeHandler")
    @patch("src.logserver.server.ClickHouseKafkaSender")
    def test_send(
        self,
        mock_clickhouse,
        mock_consume_handler,
        mock_produce_handler,
    ):
        # Arrange
        mock_kafka_produce_handler_instance = MagicMock()
        mock_kafka_consume_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_kafka_produce_handler_instance
        mock_consume_handler.return_value = mock_kafka_consume_handler_instance

        message = "test_message"
        sut = LogServer(consume_topic="consume_topic1", produce_topics=["test_topic"])

        # Act
        sut.send(uuid.uuid4(), message)

        # Assert
        mock_kafka_produce_handler_instance.produce.assert_called_once_with(
            topic="test_topic",
            data=message,
      
        )

class _StopFetching(RuntimeError):
    """Raised inside the test to break the infinite fetch loop."""
    pass
class TestFetchFromKafka(unittest.IsolatedAsyncioTestCase):
    @patch("src.logserver.server.ExactlyOnceKafkaProduceHandler")
    @patch("src.logserver.server.ExactlyOnceKafkaConsumeHandler")
    @patch("src.logserver.server.LogServer.send")
    @patch("src.logserver.server.logger")
    @patch("src.logserver.server.ClickHouseKafkaSender")
    @patch("src.logserver.server.uuid")
    async def test_fetch_from_kafka(
        self,
        mock_uuid,
        mock_clickhouse,
        mock_logger,
        mock_send,
        mock_kafka_consume,
        mock_kafka_produce,
    ):
        mock_uuid_instance = MagicMock()
        mock_uuid.return_value = mock_uuid_instance
        mock_uuid.uuid4.return_value = UUID("bd72ccb4-0ef2-4100-aa22-e787122d6875")
        mock_consume_handler = MagicMock()
        mock_consume_handler.consume.side_effect = [
            ("key1", "value1", "test-topic"),
            _StopFetching(),
        ]
        mock_kafka_consume.return_value = mock_consume_handler
        self.sut = LogServer(consume_topic="test-topic", produce_topics=["test_produce_topic"])

        # Keep real fetch but stop after _StopFetching
        original_fetch = self.sut.fetch_from_kafka
        def fetch_wrapper(*args, **kwargs):
            try:
                original_fetch(*args, **kwargs)
            except _StopFetching:
                return

        with patch.object(self.sut, "fetch_from_kafka", new=fetch_wrapper):
            self.sut.fetch_from_kafka()

        mock_send.assert_called_once_with(
            UUID("bd72ccb4-0ef2-4100-aa22-e787122d6875"), "value1"
        )

# class TestFetchFromFile(unittest.IsolatedAsyncioTestCase):

#     @patch("src.logserver.server.ExactlyOnceKafkaProduceHandler")
#     @patch("src.logserver.server.ExactlyOnceKafkaConsumeHandler")
#     @patch("src.logserver.server.LogServer.send")
#     @patch("src.logserver.server.logger")
#     @patch("src.logserver.server.ClickHouseKafkaSender")
#     async def test_fetch_from_file(
#         self,
#         mock_clickhouse,
#         mock_logger,
#         mock_send,
#         mock_kafka_consume,
#         mock_kafka_produce,
#     ):
#         self.sut = LogServer()

#         mock_send_instance = AsyncMock()
#         mock_send.return_value = mock_send_instance

#         with tempfile.NamedTemporaryFile(
#             delete=False, mode="w+", newline=""
#         ) as temp_file:
#             temp_file_path = temp_file.name
#             temp_file.write("Test line 1\n\n  \nTest line 2  \n")
#             temp_file.flush()

#         try:
#             task = asyncio.create_task(self.sut.fetch_from_file(temp_file_path))

#             await asyncio.sleep(0.2)

#             async with aiofiles.open(temp_file_path, mode="a") as f:
#                 await f.write("Test line 3\n\n")
#                 await f.write("Test line 4\n")

#             await asyncio.sleep(0.2)
#             task.cancel()
#             try:
#                 await task
#             except asyncio.CancelledError:
#                 pass
#         finally:
#             os.remove(temp_file_path)

#         self.assertEqual(2, mock_send.call_count)


class TestMain(unittest.IsolatedAsyncioTestCase):
    @patch("src.logserver.server.logger")
    @patch("src.logserver.server.LogServer")
    @patch("asyncio.create_task")  
    @patch("asyncio.run")
    @patch("src.logserver.server.SENSOR_PROTOCOLS", ["dns"])
    @patch("src.logserver.server.CONSUME_TOPIC_PREFIX", "consume_prefix")
    @patch("src.logserver.server.PRODUCE_TOPIC_PREFIX", "produce_prefix")
    @patch("src.logserver.server.COLLECTORS", [{"name": "test-collector", "protocol_base": "dns"}])
    async def test_main(self, mock_asyncio_run, mock_asyncio_create_task, mock_instance, mock_logger):
        # Arrange
        mock_instance_obj = MagicMock()
        mock_instance.return_value = mock_instance_obj
        mock_instance_obj.start = AsyncMock()
        mock_asyncio_create_task.side_effect = lambda coro: coro
        
        # Act
        await main()

        # Assert
        mock_instance_obj.start.assert_called_once()
        args, kwargs = mock_asyncio_create_task.call_args_list[0]
        expected_call = args[0]
        mock_asyncio_create_task.assert_called_once_with(expected_call)        

    @patch("src.logserver.server.logger")
    @patch("src.logserver.server.LogServer")
    @patch("asyncio.create_task")  
    @patch("asyncio.run")
    async def test_main_multiple_protocols(self, mock_asyncio_run, mock_asyncio_create_task, mock_instance, mock_logger):
        # Arrange
        mock_instance_obj = MagicMock()
        mock_instance.return_value = mock_instance_obj
        mock_instance_obj.start = AsyncMock()
        mock_asyncio_create_task.side_effect = lambda coro: coro

        mock_asyncio_run.side_effect = RuntimeError("simulated failure")

        # Act & Assert
        await main()

        args, kwargs = mock_asyncio_create_task.call_args_list[0]
        expected_call = args[0]
        assert mock_asyncio_create_task.call_count == 2
if __name__ == "__main__":
    unittest.main()
