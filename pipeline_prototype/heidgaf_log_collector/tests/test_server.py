import unittest
from ipaddress import IPv4Address

from pipeline_prototype.heidgaf_log_collector.server import LogServer


class TestInit(unittest.TestCase):
    def test_valid_init(self):
        host = "192.168.0.1"
        port_in = 9998
        port_out = 9999

        server_instance = LogServer(host, port_in, port_out)
        self.assertEqual(IPv4Address(host), server_instance.host)
        self.assertEqual(port_in, server_instance.port_in)
        self.assertEqual(port_out, server_instance.port_out)
        self.assertTrue(server_instance.data_queue.empty())

    def test_invalid_init_with_no_port(self):
        with self.assertRaises(TypeError):
            # noinspection PyArgumentList
            LogServer(host="192.168.2.1")

    def test_invalid_init_with_one_port(self):
        with self.assertRaises(TypeError):
            # noinspection PyArgumentList
            LogServer(port_in=9999)

    def test_invalid_init_with_only_ports(self):
        with self.assertRaises(TypeError):
            # noinspection PyArgumentList
            LogServer(port_in=9998, port_out=9999)

    def test_invalid_init_with_invalid_host(self):
        with self.assertRaises(ValueError):
            LogServer("256.256.256.256", 9998, 9999)


class TestGetNextLogline(unittest.TestCase):
    def test_valid(self):
        server_instance = LogServer("127.0.0.1", 9998, 9999)
        server_instance.data_queue.put("Element 1")
        server_instance.data_queue.put("Element 2")

        self.assertEqual("Element 1", server_instance.get_next_logline())
        self.assertEqual("Element 2", server_instance.get_next_logline())

    def test_valid_from_empty_queue(self):
        server_instance = LogServer("127.0.0.1", 9998, 9999)
        self.assertEqual(None, server_instance.get_next_logline())


if __name__ == '__main__':
    unittest.main()
