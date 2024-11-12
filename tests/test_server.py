import asyncio
import os
import tempfile
import unittest
from ipaddress import IPv4Address, IPv6Address
from unittest.mock import AsyncMock, MagicMock, patch

import aiofiles

from src.logserver.server import LogServer, main

LOG_SERVER_IP_ADDR = "192.168.0.1"
LOG_SERVER_PORT_IN = 9998
LOG_SERVER_PORT_OUT = 9999


class TestInit(unittest.TestCase):
    @patch("src.logserver.server.HOSTNAME", "127.0.0.1")
    @patch("src.logserver.server.LISTEN_ON_TOPIC", "test_topic")
    @patch("src.logserver.server.Lock")
    @patch("src.logserver.server.SimpleKafkaProduceHandler")
    @patch("src.logserver.server.SimpleKafkaConsumeHandler")
    def test_valid_init_ipv4(
        self, mock_kafka_consume_handler, mock_kafka_produce_handler, mock_lock
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_produce_handler_instance = MagicMock()
        mock_lock_instance = MagicMock()

        mock_kafka_produce_handler.return_value = mock_kafka_produce_handler_instance
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_lock.return_value = mock_lock_instance

        sut = LogServer()
        self.assertEqual(IPv4Address("127.0.0.1"), sut.host)
        self.assertEqual(mock_lock_instance, sut.lock)
        self.assertEqual(mock_kafka_consume_handler_instance, sut.kafka_consume_handler)
        self.assertEqual(mock_kafka_produce_handler_instance, sut.kafka_produce_handler)
        mock_kafka_consume_handler.assert_called_once_with(topics="test_topic")

    @patch("src.logserver.server.HOSTNAME", "fe80::1")
    @patch("src.logserver.server.LISTEN_ON_TOPIC", "test_topic")
    @patch("src.logserver.server.Lock")
    @patch("src.logserver.server.SimpleKafkaProduceHandler")
    @patch("src.logserver.server.SimpleKafkaConsumeHandler")
    def test_valid_init_ipv6(
        self, mock_kafka_consume_handler, mock_kafka_produce_handler, mock_lock
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_produce_handler_instance = MagicMock()
        mock_lock_instance = MagicMock()

        mock_kafka_produce_handler.return_value = mock_kafka_produce_handler_instance
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_lock.return_value = mock_lock_instance

        sut = LogServer()
        self.assertEqual(IPv6Address("fe80::1"), sut.host)
        self.assertEqual(mock_lock_instance, sut.lock)
        self.assertEqual(mock_kafka_consume_handler_instance, sut.kafka_consume_handler)
        self.assertEqual(mock_kafka_produce_handler_instance, sut.kafka_produce_handler)
        mock_kafka_consume_handler.assert_called_once_with(topics="test_topic")

    @patch("src.logserver.server.HOSTNAME", "256.256.256.256")
    @patch("src.logserver.server.LISTEN_ON_TOPIC", "test_topic")
    @patch("src.logserver.server.Lock")
    @patch("src.logserver.server.SimpleKafkaProduceHandler")
    @patch("src.logserver.server.SimpleKafkaConsumeHandler")
    def test_invalid_init_with_invalid_host(
        self, mock_kafka_consume_handler, mock_kafka_produce_handler, mock_lock
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_produce_handler_instance = MagicMock()
        mock_lock_instance = MagicMock()

        mock_kafka_produce_handler.return_value = mock_kafka_produce_handler_instance
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_lock.return_value = mock_lock_instance

        with self.assertRaises(ValueError):
            LogServer()

        mock_kafka_consume_handler.assert_not_called()
        mock_kafka_produce_handler.assert_not_called()


class TestOpen(unittest.IsolatedAsyncioTestCase):
    @patch("src.logserver.server.HOSTNAME", "127.0.0.1")
    @patch("src.logserver.server.logger")
    @patch("src.logserver.server.LogServer.fetch_from_kafka")
    @patch("src.logserver.server.LogServer.fetch_from_file")
    @patch("src.logserver.server.SimpleKafkaConsumeHandler")
    async def test_open(
        self,
        mock_kafka_consume_handler,
        mock_fetch_from_file,
        mock_fetch_from_kafka,
        mock_logger,
    ):
        # Arrange
        sut = LogServer()

        # Act
        await sut.start()

        # Assert
        mock_fetch_from_kafka.assert_called_once()
        mock_fetch_from_file.assert_called_once()


class TestFetchFromKafka(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.sut = LogServer()
        self.sut.kafka_consume_handler = AsyncMock()
        self.sut.kafka_produce_handler = AsyncMock()

    @patch("src.logserver.server.SEND_TO_TOPIC", "test_topic")
    @patch("src.logserver.server.logger")
    @patch("asyncio.get_running_loop")
    async def test_handle_kafka_inputs(self, mock_get_running_loop, mock_logger):
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

        self.sut.kafka_produce_handler.produce.assert_called_once_with(
            topic="test_topic", data="value1"
        )


class TestFetchFromFile(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.sut = LogServer()
        self.sut.kafka_consume_handler = AsyncMock()
        self.sut.kafka_produce_handler = AsyncMock()

    @patch("src.logserver.server.SEND_TO_TOPIC", "test_topic")
    @patch("src.logserver.server.logger")
    async def test_fetch_from_file(self, mock_logger):
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

        self.sut.kafka_produce_handler.produce.assert_any_call(
            topic="test_topic", data="Test line 3"
        )
        self.sut.kafka_produce_handler.produce.assert_any_call(
            topic="test_topic", data="Test line 4"
        )


class TestMainFunction(unittest.TestCase):
    @patch("src.logserver.server.logger")
    @patch("src.logserver.server.asyncio.run")
    @patch("src.logserver.server.LogServer")
    def test_main(self, mock_log_server_class, mock_asyncio_run, mock_logger):
        # Arrange
        mock_server_instance = MagicMock()
        mock_log_server_class.return_value = mock_server_instance

        # Act
        main()

        # Assert
        mock_log_server_class.assert_called_once()
        mock_server_instance.start.assert_called_once()
        mock_asyncio_run.assert_called_once_with(mock_server_instance.start())


if __name__ == "__main__":
    unittest.main()
