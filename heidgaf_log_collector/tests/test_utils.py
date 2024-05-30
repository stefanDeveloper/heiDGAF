import ipaddress
import unittest

from heidgaf_log_collector.utils import validate_host
from heidgaf_log_collector.utils import validate_port


class TestValidateHosts(unittest.TestCase):
    def test_valid_as_ip_address_type(self):
        valid_ip = ipaddress.ip_address('192.168.0.1')
        self.assertEqual(validate_host(valid_ip), ipaddress.IPv4Address('192.168.0.1'))

    def test_valid_as_string(self):
        valid_ip = "192.168.0.1"
        self.assertEqual(validate_host(valid_ip), ipaddress.IPv4Address('192.168.0.1'))

    def test_valid_as_ipv4_address_type(self):
        valid_ip = ipaddress.IPv4Address('192.168.0.1')
        self.assertEqual(validate_host(valid_ip), ipaddress.IPv4Address('192.168.0.1'))

    def test_valid_as_ipv6_address_type(self):
        valid_ip = ipaddress.IPv6Address('fe80::1')
        self.assertEqual(validate_host(valid_ip), ipaddress.IPv6Address('fe80::1'))

    def test_invalid_ip(self):
        invalid_ip = '256.256.256.256'
        with self.assertRaises(ValueError):
            validate_host(invalid_ip)

    def test_invalid_address(self):
        invalid_host = 'example.com'
        with self.assertRaises(ValueError):
            validate_host(invalid_host)


class TestValidatePort(unittest.TestCase):
    def test_valid(self):
        valid_port = 8080
        self.assertEqual(validate_port(valid_port), valid_port)

    def test_valid_upper_edge(self):
        valid_port = 65535
        self.assertEqual(validate_port(valid_port), valid_port)

    def test_valid_lower_edge(self):
        valid_port = 1
        self.assertEqual(validate_port(valid_port), valid_port)

    def test_invalid_small_port(self):
        small_port = 0
        with self.assertRaises(ValueError):
            validate_port(small_port)

    def test_invalid_large_port(self):
        large_port = 65536
        with self.assertRaises(ValueError):
            validate_port(large_port)


if __name__ == '__main__':
    unittest.main()
