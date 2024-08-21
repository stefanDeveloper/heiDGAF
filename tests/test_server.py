import asyncio
import unittest
from ipaddress import IPv4Address, IPv6Address
from unittest.mock import AsyncMock, MagicMock, patch

from src.logserver.server import LogServer

LOG_SERVER_IP_ADDR = "192.168.0.1"
LOG_SERVER_PORT_IN = 9998
LOG_SERVER_PORT_OUT = 9999


class TestInit(unittest.TestCase):
    @patch("src.logserver.server.HOSTNAME", "127.0.0.1")
    @patch("src.logserver.server.PORT_IN", 7777)
    @patch("src.logserver.server.PORT_OUT", 8888)
    def test_valid_init_ipv4(self):
        server_instance = LogServer()
        self.assertEqual(IPv4Address("127.0.0.1"), server_instance.host)
        self.assertEqual(7777, server_instance.port_in)
        self.assertEqual(8888, server_instance.port_out)
        self.assertTrue(server_instance.data_queue.empty())
        self.assertEqual(0, server_instance.number_of_connections)

    @patch("src.logserver.server.HOSTNAME", "fe80::1")
    @patch("src.logserver.server.PORT_IN", 7777)
    @patch("src.logserver.server.PORT_OUT", 8888)
    def test_valid_init_ipv6(self):
        server_instance = LogServer()
        self.assertEqual(IPv6Address("fe80::1"), server_instance.host)
        self.assertEqual(7777, server_instance.port_in)
        self.assertEqual(8888, server_instance.port_out)
        self.assertTrue(server_instance.data_queue.empty())
        self.assertEqual(0, server_instance.number_of_connections)

    @patch("src.logserver.server.HOSTNAME", "256.256.256.256")
    @patch("src.logserver.server.PORT_IN", 7777)
    @patch("src.logserver.server.PORT_OUT", 8888)
    def test_invalid_init_with_invalid_host(self):
        with self.assertRaises(ValueError):
            LogServer()


# TODO: Update
# class TestOpen(unittest.IsolatedAsyncioTestCase):
#     @patch("src.logserver.server.HOSTNAME", "127.0.0.1")
#     @patch("src.logserver.server.PORT_IN", 12345)
#     @patch("src.logserver.server.PORT_OUT", 12346)
#     @patch("src.logserver.server.asyncio.start_server")
#     async def test_open(self, mock_start_server):
#         sut = LogServer()
#
#         send_server = AsyncMock()
#         receive_server = AsyncMock()
#         mock_start_server.side_effect = [send_server, receive_server]
#
#         async def mock_serve_forever():
#             await asyncio.sleep(0)  # Simulate an async operation
#
#         send_server.serve_forever = mock_serve_forever
#         receive_server.serve_forever = mock_serve_forever
#
#         send_server.close = AsyncMock()
#         receive_server.close = AsyncMock()
#         send_server.wait_closed = AsyncMock()
#         receive_server.wait_closed = AsyncMock()
#
#         async def run_open():
#             await sut.open()
#
#         open_task = asyncio.create_task(run_open())
#         await asyncio.sleep(0.1)  # Let the server run for a brief moment
#         open_task.cancel()  # Simulate a KeyboardInterrupt
#
#         try:
#             await open_task
#         except asyncio.CancelledError:
#             pass
#
#         mock_start_server.assert_any_call(
#             sut.handle_send_logline, "127.0.0.1", 12345
#         )
#         mock_start_server.assert_any_call(
#             sut.handle_receive_logline, "127.0.0.1", 12346
#         )
#
#         send_server.close.assert_called_once()
#         receive_server.close.assert_called_once()
#         send_server.wait_closed.assert_awaited_once()
#         receive_server.wait_closed.assert_awaited_once()


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
        self.assertEqual(
            5, server_instance.number_of_connections
        )


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
    async def test_send_logline_with_logline(self):
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
    async def test_receive_logline(self):
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


if __name__ == "__main__":
    unittest.main()
