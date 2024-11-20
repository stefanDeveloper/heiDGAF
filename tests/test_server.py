import asyncio
import os
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import aiofiles

from src.logserver.server import LogServer, main

LOG_SERVER_IP_ADDR = "192.168.0.1"


class TestInit(unittest.TestCase):
    @patch("src.logserver.server.CONSUME_TOPIC", "test_topic")
    @patch("src.logserver.server.ExactlyOnceKafkaProduceHandler")
    @patch("src.logserver.server.SimpleKafkaConsumeHandler")
    def test_valid_init(self, mock_kafka_consume_handler, mock_kafka_produce_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_produce_handler_instance = MagicMock()

        mock_kafka_produce_handler.return_value = mock_kafka_produce_handler_instance
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = LogServer()
        self.assertEqual(mock_kafka_consume_handler_instance, sut.kafka_consume_handler)
        self.assertEqual(mock_kafka_produce_handler_instance, sut.kafka_produce_handler)
        mock_kafka_consume_handler.assert_called_once_with("test_topic")


class TestStart(unittest.IsolatedAsyncioTestCase):
    @patch("src.logserver.server.logger")
    @patch("src.logserver.server.SimpleKafkaConsumeHandler")
    @patch("src.logserver.server.ExactlyOnceKafkaProduceHandler")
    def setUp(
        self,
        mock_kafka_produce_handler,
        mock_kafka_consume_handler,
        mock_logger,
    ):
        self.sut = LogServer()

    @patch("src.logserver.server.LogServer.fetch_from_kafka")
    @patch("src.logserver.server.LogServer.fetch_from_file")
    async def test_start(
        self,
        mock_fetch_from_file,
        mock_fetch_from_kafka,
    ):
        # Act
        await self.sut.start()

        # Assert
        mock_fetch_from_kafka.assert_called_once()
        mock_fetch_from_file.assert_called_once()

    # TODO: Update
    # @patch("src.logserver.server.LogServer.fetch_from_kafka")
    # @patch("src.logserver.server.LogServer.fetch_from_file")
    # async def test_start_handles_keyboard_interrupt(
    #         self,
    #         mock_fetch_from_file,
    #         mock_fetch_from_kafka,
    # ):
    #     # Arrange
    #     async def mock_gather(*args, **kwargs):
    #         raise KeyboardInterrupt
    #
    #     with (patch('src.logserver.server.asyncio.gather', side_effect=mock_gather) as mock):
    #         # Act
    #         await self.sut.start()
    #
    #         # Assert
    #         mock.assert_called_once()
    #         mock_fetch_from_kafka.assert_called_once()
    #         mock_fetch_from_file.assert_called_once()


class TestSend(unittest.TestCase):
    @patch("src.logserver.server.PRODUCE_TOPIC", "test_topic")
    @patch("src.logserver.server.ExactlyOnceKafkaProduceHandler")
    def test_send(
        self,
        mock_produce_handler,
    ):
        # Arrange
        mock_kafka_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_kafka_produce_handler_instance

        message = "test_message"
        sut = LogServer()

        # Act
        sut.send(message)

        # Assert
        mock_kafka_produce_handler_instance.produce.assert_called_once_with(
            topic="test_topic",
            data=message,
        )


class TestFetchFromKafka(unittest.IsolatedAsyncioTestCase):
    @patch("src.logserver.server.ExactlyOnceKafkaProduceHandler")
    @patch("src.logserver.server.SimpleKafkaConsumeHandler")
    @patch("src.logserver.server.LogServer.send")
    @patch("src.logserver.server.logger")
    @patch("asyncio.get_running_loop")
    async def test_handle_kafka_inputs(
        self,
        mock_get_running_loop,
        mock_logger,
        mock_send,
        mock_kafka_consume,
        mock_kafka_produce,
    ):
        self.sut = LogServer()

        mock_send_instance = AsyncMock()
        mock_send.return_value = mock_send_instance
        mock_loop = AsyncMock()
        mock_get_running_loop.return_value = mock_loop
        self.sut.kafka_consume_handler.consume.return_value = (
            "key1",
            "value1",
            "topic1",
        )

        mock_loop.run_in_executor.side_effect = [
            ("key1", "value1", "topic1"),
            asyncio.CancelledError(),
        ]

        with self.assertRaises(asyncio.CancelledError):
            await self.sut.fetch_from_kafka()

        mock_send.assert_called_once_with("value1")


class TestFetchFromFile(unittest.IsolatedAsyncioTestCase):

    @patch("src.logserver.server.ExactlyOnceKafkaProduceHandler")
    @patch("src.logserver.server.SimpleKafkaConsumeHandler")
    @patch("src.logserver.server.PRODUCE_TOPIC", "test_topic")
    @patch("src.logserver.server.LogServer.send")
    @patch("src.logserver.server.logger")
    async def test_fetch_from_file(
        self, mock_logger, mock_send, mock_kafka_consume, mock_kafka_produce
    ):
        self.sut = LogServer()

        mock_send_instance = AsyncMock()
        mock_send.return_value = mock_send_instance

        with tempfile.NamedTemporaryFile(
            delete=False, mode="w+", newline=""
        ) as temp_file:
            temp_file_path = temp_file.name
            temp_file.write("Test line 1\n\n  \nTest line 2  \n")
            temp_file.flush()

        try:
            task = asyncio.create_task(self.sut.fetch_from_file(temp_file_path))

            await asyncio.sleep(0.2)

            async with aiofiles.open(temp_file_path, mode="a") as f:
                await f.write("Test line 3\n\n")
                await f.write("Test line 4\n")

            await asyncio.sleep(0.2)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        finally:
            os.remove(temp_file_path)

        mock_send.assert_any_call("Test line 3")
        mock_send.assert_any_call("Test line 4")


class TestMain(unittest.TestCase):
    @patch("src.logserver.server.logger")
    @patch("src.logserver.server.LogServer")
    @patch("asyncio.run")
    def test_main(self, mock_asyncio_run, mock_instance, mock_logger):
        # Arrange
        mock_instance_obj = MagicMock()
        mock_instance.return_value = mock_instance_obj

        # Act
        main()

        # Assert
        mock_instance.assert_called_once()
        mock_asyncio_run.assert_called_once_with(mock_instance_obj.start())


if __name__ == "__main__":
    unittest.main()
