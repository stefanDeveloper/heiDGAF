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
    @patch("src.logserver.server.PORT_IN", 7777)
    @patch("src.logserver.server.PORT_OUT", 8888)
    @patch("src.logserver.server.LISTEN_ON_TOPIC", "test_topic")
    @patch("src.logserver.server.KafkaConsumeHandler")
    def test_valid_init_ipv4(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = LogServer()
        self.assertEqual(IPv4Address("127.0.0.1"), sut.host)
        self.assertEqual(7777, sut.port_in)
        self.assertEqual(8888, sut.port_out)
        self.assertTrue(sut.data_queue.empty())
        self.assertEqual(0, sut.number_of_connections)
        self.assertEqual(mock_kafka_consume_handler_instance, sut.kafka_consume_handler)
        mock_kafka_consume_handler.assert_called_once_with(topic="test_topic")

    @patch("src.logserver.server.HOSTNAME", "fe80::1")
    @patch("src.logserver.server.PORT_IN", 7777)
    @patch("src.logserver.server.PORT_OUT", 8888)
    @patch("src.logserver.server.LISTEN_ON_TOPIC", "test_topic")
    @patch("src.logserver.server.KafkaConsumeHandler")
    def test_valid_init_ipv6(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = LogServer()
        self.assertEqual(IPv6Address("fe80::1"), sut.host)
        self.assertEqual(7777, sut.port_in)
        self.assertEqual(8888, sut.port_out)
        self.assertTrue(sut.data_queue.empty())
        self.assertEqual(0, sut.number_of_connections)
        self.assertEqual(mock_kafka_consume_handler_instance, sut.kafka_consume_handler)
        mock_kafka_consume_handler.assert_called_once_with(topic="test_topic")

    @patch("src.logserver.server.HOSTNAME", "256.256.256.256")
    @patch("src.logserver.server.PORT_IN", 7777)
    @patch("src.logserver.server.PORT_OUT", 8888)
    @patch("src.logserver.server.LISTEN_ON_TOPIC", "test_topic")
    @patch("src.logserver.server.KafkaConsumeHandler")
    def test_invalid_init_with_invalid_host(self, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        with self.assertRaises(ValueError):
            LogServer()

        mock_kafka_consume_handler.assert_not_called()


class TestOpen(unittest.IsolatedAsyncioTestCase):
    @patch("src.logserver.server.logger")
    @patch("src.logserver.server.HOSTNAME", "127.0.0.1")
    @patch("src.logserver.server.PORT_IN", 1234)
    @patch("src.logserver.server.PORT_OUT", 5678)
    @patch("src.logserver.server.LogServer.handle_kafka_inputs")
    @patch("src.logserver.server.LogServer.async_follow")
    @patch("src.logserver.server.KafkaConsumeHandler")
    async def test_open(
        self, mock_kafka_consume_handler, mock_follow, mock_handle_kafka, mock_logger
    ):
        # Arrange
        sut = LogServer()

        with patch("asyncio.start_server", new_callable=AsyncMock) as mock_start_server:
            mock_send_server = MagicMock()
            mock_receive_server = MagicMock()

            mock_start_server.side_effect = [mock_send_server, mock_receive_server]

            mock_send_server.serve_forever = AsyncMock()
            mock_receive_server.serve_forever = AsyncMock()
            mock_send_server.wait_closed = AsyncMock()
            mock_receive_server.wait_closed = AsyncMock()

            # Act
            await sut.open()

            # Assert
            mock_start_server.assert_any_call(
                sut.handle_send_logline, "127.0.0.1", 5678
            )
            mock_start_server.assert_any_call(
                sut.handle_receive_logline, "127.0.0.1", 1234
            )
            mock_send_server.serve_forever.assert_awaited_once()
            mock_receive_server.serve_forever.assert_awaited_once()
            mock_send_server.close.assert_called_once()
            mock_receive_server.close.assert_called_once()
            mock_send_server.wait_closed.assert_awaited_once()
            mock_receive_server.wait_closed.assert_awaited_once()
            mock_handle_kafka.assert_called_once()
            mock_follow.assert_called_once()

    @patch("src.logserver.server.logger")
    @patch("src.logserver.server.HOSTNAME", "127.0.0.1")
    @patch("src.logserver.server.PORT_IN", 1234)
    @patch("src.logserver.server.PORT_OUT", 5678)
    async def test_open_keyboard_interrupt(self, mock_logger):
        # Arrange
        sut = LogServer()

        with patch("asyncio.start_server", new_callable=AsyncMock) as mock_start_server:
            mock_send_server = MagicMock()
            mock_receive_server = MagicMock()

            mock_start_server.side_effect = [mock_send_server, mock_receive_server]

            mock_send_server.serve_forever.side_effect = KeyboardInterrupt
            mock_receive_server.serve_forever = AsyncMock()
            mock_send_server.wait_closed = AsyncMock()
            mock_receive_server.wait_closed = AsyncMock()

            # Act & Assert
            await sut.open()

            # Additional Assertions
            mock_send_server.close.assert_called_once()
            mock_receive_server.close.assert_called_once()
            mock_send_server.wait_closed.assert_awaited_once()
            mock_receive_server.wait_closed.assert_awaited_once()


class TestHandleConnection(unittest.IsolatedAsyncioTestCase):
    async def test_handle_connection_sending(self):
        server_instance = LogServer()
        server_instance.send_logline = AsyncMock()
        server_instance.get_next_logline = MagicMock(return_value="test logline")

        reader = AsyncMock()
        writer = AsyncMock()
        writer.get_extra_info = MagicMock(return_value="test_address")

        await server_instance.handle_connection(reader, writer, sending=True)

        server_instance.send_logline.assert_awaited_once_with(writer, "test logline")
        writer.close.assert_called_once()
        writer.wait_closed.assert_awaited_once()
        self.assertEqual(0, server_instance.number_of_connections)

    async def test_handle_connection_receiving(self):
        server_instance = LogServer()
        server_instance.receive_logline = AsyncMock()

        reader = AsyncMock()
        writer = AsyncMock()
        writer.get_extra_info = MagicMock(return_value="test_address")

        await server_instance.handle_connection(reader, writer, sending=False)

        server_instance.receive_logline.assert_awaited_once_with(reader)
        writer.close.assert_called_once()
        writer.wait_closed.assert_awaited_once()
        self.assertEqual(0, server_instance.number_of_connections)

    async def test_handle_connection_rejected(self):
        server_instance = LogServer()
        server_instance.number_of_connections = 5

        reader = AsyncMock()
        writer = AsyncMock()
        writer.get_extra_info = MagicMock(return_value="test_address")

        await server_instance.handle_connection(reader, writer, sending=True)

        writer.close.assert_called_once()
        writer.wait_closed.assert_awaited_once()
        self.assertEqual(5, server_instance.number_of_connections)

    async def test_handle_connection_increases_and_decreases_connections(self):
        server_instance = LogServer()
        server_instance.send_logline = AsyncMock()
        server_instance.get_next_logline = MagicMock(return_value="test logline")
        server_instance.number_of_connections = 3

        reader = AsyncMock()
        writer = AsyncMock()
        writer.get_extra_info = MagicMock(return_value="test_address")

        await server_instance.handle_connection(reader, writer, sending=True)

        self.assertEqual(3, server_instance.number_of_connections)

    async def test_handle_connection_cancelled_error(self):
        server_instance = LogServer()
        server_instance.send_logline = AsyncMock(side_effect=asyncio.CancelledError)
        server_instance.get_next_logline = MagicMock(return_value="test logline")

        reader = AsyncMock()
        writer = AsyncMock()
        writer.get_extra_info = MagicMock(return_value="test_address")

        await server_instance.handle_connection(reader, writer, sending=True)

        server_instance.send_logline.assert_awaited_once_with(writer, "test logline")
        writer.close.assert_called_once()
        writer.wait_closed.assert_awaited_once()
        self.assertEqual(0, server_instance.number_of_connections)

    @patch("src.logserver.server.logger")
    @patch("src.logserver.server.MAX_NUMBER_OF_CONNECTIONS", 7)
    async def test_handle_connection_rejects_additional_connections(self, mock_logger):
        server_instance = LogServer()
        server_instance.number_of_connections = 7

        reader = AsyncMock()
        writer = AsyncMock()
        writer.get_extra_info = MagicMock(return_value="test_address")

        await server_instance.handle_connection(reader, writer, sending=True)

        writer.close.assert_called_once()
        writer.wait_closed.assert_awaited_once()
        self.assertEqual(7, server_instance.number_of_connections)


class TestHandleKafkaInputs(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.sut = LogServer()
        self.sut.kafka_consume_handler = AsyncMock()
        self.sut.data_queue = MagicMock()

    @patch("src.logserver.server.logger")
    @patch("asyncio.get_running_loop")
    async def test_handle_kafka_inputs(self, mock_get_running_loop, mock_logger):
        mock_loop = AsyncMock()
        mock_get_running_loop.return_value = mock_loop
        self.sut.kafka_consume_handler.consume.return_value = ("key1", "value1")

        mock_loop.run_in_executor.side_effect = [
            ("key1", "value1"),
            asyncio.CancelledError(),
        ]

        with self.assertRaises(asyncio.CancelledError):
            await self.sut.handle_kafka_inputs()

        self.sut.data_queue.put.assert_called_once_with("value1")


class TestAsyncFollow(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.sut = LogServer()
        self.sut.kafka_consume_handler = AsyncMock()
        self.sut.data_queue = MagicMock()

    @patch("src.logserver.server.logger")
    async def test_async_follow(self, mock_logger):
        with tempfile.NamedTemporaryFile(
            delete=False, mode="w+", newline=""
        ) as temp_file:
            temp_file_path = temp_file.name
            temp_file.write("Test line 1\n\n  \nTest line 2  \n")
            temp_file.flush()

        try:
            task = asyncio.create_task(self.sut.async_follow(temp_file_path))

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

        self.sut.data_queue.put.assert_any_call("Test line 3")
        self.sut.data_queue.put.assert_any_call("Test line 4")


class TestHandleSendLogline(unittest.IsolatedAsyncioTestCase):
    async def test_handle_send_logline(self):
        server_instance = LogServer()
        server_instance.handle_connection = AsyncMock()

        reader = AsyncMock()
        writer = AsyncMock()

        await server_instance.handle_send_logline(reader, writer)

        server_instance.handle_connection.assert_awaited_once_with(reader, writer, True)


class TestHandleReceiveLogline(unittest.IsolatedAsyncioTestCase):
    async def test_handle_receive_logline(self):
        server_instance = LogServer()
        server_instance.handle_connection = AsyncMock()

        reader = AsyncMock()
        writer = AsyncMock()

        await server_instance.handle_receive_logline(reader, writer)

        server_instance.handle_connection.assert_awaited_once_with(
            reader, writer, False
        )


class TestSendLogline(unittest.IsolatedAsyncioTestCase):
    @patch("src.logserver.server.logger")
    async def test_send_logline_with_logline(self, mock_logger):
        server_instance = LogServer()
        writer = AsyncMock()
        logline = "Test logline"

        await server_instance.send_logline(writer, logline)

        writer.write.assert_called_once_with(logline.encode("utf-8"))
        writer.drain.assert_called_once()

    async def test_send_logline_no_logline(self):
        server_instance = LogServer()
        writer = AsyncMock()
        logline = ""

        await server_instance.send_logline(writer, logline)

        writer.write.assert_not_called()
        writer.drain.assert_not_called()


class TestReceiveLogline(unittest.IsolatedAsyncioTestCase):
    @patch("src.logserver.server.logger")
    async def test_receive_logline(self, mock_logger):
        reader = AsyncMock()
        data_queue = MagicMock()
        server_instance = LogServer()
        server_instance.data_queue = data_queue

        reader.read = AsyncMock(side_effect=[b"Test message 1", b"Test message 2", b""])

        receive_task = asyncio.create_task(server_instance.receive_logline(reader))
        await receive_task

        data_queue.put.assert_any_call("Test message 1")
        data_queue.put.assert_any_call("Test message 2")

        self.assertEqual(data_queue.put.call_count, 2)


class TestGetNextLogline(unittest.TestCase):
    def test_valid(self):
        server_instance = LogServer()
        server_instance.data_queue.put("Element 1")
        server_instance.data_queue.put("Element 2")

        self.assertEqual("Element 1", server_instance.get_next_logline())
        self.assertEqual("Element 2", server_instance.get_next_logline())

    def test_valid_from_empty_queue(self):
        server_instance = LogServer()
        self.assertIsNone(server_instance.get_next_logline())


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
        mock_server_instance.open.assert_called_once()
        mock_asyncio_run.assert_called_once_with(mock_server_instance.open())


if __name__ == "__main__":
    unittest.main()
