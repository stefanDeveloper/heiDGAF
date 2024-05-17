import ipaddress
import unittest

from pipeline_prototype.heidgaf_log_collector.server import Server


class TestServer(unittest.TestCase):
    def test_init(self):
        host = "192.168.0.1"
        port = 9999
        server_instance = Server(host, port)
        self.assertEqual(server_instance.host, ipaddress.IPv4Address("192.168.0.1"))
        self.assertEqual(server_instance.port, 9999)

    def test_init_with_no_host(self):
        with self.assertRaises(TypeError):
            # noinspection PyArgumentList
            Server(port=9999)

    def test_init_with_no_port(self):
        with self.assertRaises(TypeError):
            # noinspection PyArgumentList
            Server(host="localhost")

    def test_init_with_invalid_host(self):
        with self.assertRaises(ValueError):
            Server("256.256.256.256", 9999)

    def test_init_with_invalid_host_type(self):
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            Server(123.5, 9999)

    def test_init_with_invalid_port(self):
        with self.assertRaises(ValueError):
            Server("192.168.0.1", 70000)

    def test_init_with_invalid_port_type(self):
        with self.assertRaises(TypeError):
            # noinspection PyTypeChecker
            Server("172.0.0.1", "9999")

    # TODO: Add tests for other methods


if __name__ == '__main__':
    unittest.main()
