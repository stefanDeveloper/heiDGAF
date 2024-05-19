import ipaddress
import unittest

from pipeline_prototype.heidgaf_log_collector.server import LogServer


class TestServer(unittest.TestCase):
    # TESTS FOR __init__
    def test_init(self):
        host = "192.168.0.1"
        port = 9999
        server_instance = LogServer(host, port)
        self.assertEqual(server_instance.host, ipaddress.IPv4Address("192.168.0.1"))
        self.assertEqual(server_instance.send_port, 9999)

    def test_init_with_no_host(self):
        with self.assertRaises(TypeError):
            # noinspection PyArgumentList
            LogServer(send_port=9999)

    def test_init_with_no_port(self):
        with self.assertRaises(TypeError):
            # noinspection PyArgumentList
            LogServer(host="localhost")

    def test_init_with_invalid_host(self):
        with self.assertRaises(ValueError):
            LogServer("256.256.256.256", 9999)

    def test_init_with_invalid_host_type(self):
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            LogServer(123.5, 9999)

    def test_init_with_invalid_port(self):
        with self.assertRaises(ValueError):
            LogServer("192.168.0.1", 70000)

    def test_init_with_invalid_port_type(self):
        with self.assertRaises(TypeError):
            # noinspection PyTypeChecker
            LogServer("127.0.0.1", "9999")

    # TESTS FOR get_next_logline
    def test_get_next_logline(self):
        server_instance = LogServer("127.0.0.1", 9999)
        self.assertEqual(server_instance.get_next_logline(), "this is a mock logline")

    # TODO: Add tests for other methods


if __name__ == '__main__':
    unittest.main()
