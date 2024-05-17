import ipaddress
import unittest

from pipeline_prototype.heidgaf_log_collector.utils import validate_host
from pipeline_prototype.heidgaf_log_collector.utils import validate_port


class TestUtils(unittest.TestCase):
    def test_validate_host_valid_as_ip_address_type(self):
        valid_ip = ipaddress.ip_address('192.168.0.1')
        self.assertEqual(validate_host(valid_ip), ipaddress.IPv4Address('192.168.0.1'))

    def test_validate_host_valid_as_string(self):
        valid_ip = "192.168.0.1"
        self.assertEqual(validate_host(valid_ip), ipaddress.IPv4Address('192.168.0.1'))

    def test_validate_host_valid_as_ipv4_address_type(self):
        valid_ip = ipaddress.IPv4Address('192.168.0.1')
        self.assertEqual(validate_host(valid_ip), ipaddress.IPv4Address('192.168.0.1'))

    def test_validate_host_valid_as_ipv6_address_type(self):
        valid_ip = ipaddress.IPv6Address('fe80::1')
        self.assertEqual(validate_host(valid_ip), ipaddress.IPv6Address('fe80::1'))

    def test_validate_host_invalid_ip(self):
        invalid_ip = '256.256.256.256'
        with self.assertRaises(ValueError):
            validate_host(invalid_ip)

    def test_validate_host_invalid_type(self):
        invalid_ip = 190.5  # type float
        with self.assertRaises(ValueError):
            validate_host(invalid_ip)

    def test_validate_host_invalid_address(self):
        invalid_host = 'example.com'
        with self.assertRaises(ValueError):
            validate_host(invalid_host)

    def test_validate_port_valid(self):
        valid_port = 8080
        self.assertEqual(validate_port(valid_port), valid_port)

    def test_validate_port_valid_upper_edge(self):
        valid_port = 65535
        self.assertEqual(validate_port(valid_port), valid_port)

    def test_validate_port_valid_lower_edge(self):
        valid_port = 1
        self.assertEqual(validate_port(valid_port), valid_port)

    def test_validate_port_invalid_small_port(self):
        small_port = 0
        with self.assertRaises(ValueError):
            validate_port(small_port)

    def test_validate_port_invalid_large_port(self):
        large_port = 65536
        with self.assertRaises(ValueError):
            validate_port(large_port)

    def test_validate_port_invalid_type(self):
        invalid_port = 123.5
        with self.assertRaises(TypeError):
            # noinspection PyTypeChecker
            validate_port(invalid_port)


if __name__ == '__main__':
    unittest.main()
