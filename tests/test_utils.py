import unittest
from unittest.mock import patch, mock_open, MagicMock

from src.base.utils import *


class TestSetupConfig(unittest.TestCase):
    @patch("src.base.utils.CONFIG_FILEPATH", "fake/path/config.yaml")
    @patch("builtins.open", new_callable=mock_open, read_data="some_yaml_data: value")
    @patch("yaml.safe_load", return_value={"some_yaml_data": "value"})
    def test_load_config_success(self, mock_yaml_safe_load, mock_open_file):
        result = setup_config()

        mock_open_file.assert_called_once_with("fake/path/config.yaml", "r")
        mock_yaml_safe_load.assert_called_once()
        self.assertEqual(result, {"some_yaml_data": "value"})

    @patch("src.base.utils.CONFIG_FILEPATH", "fake/path/config.yaml")
    @patch("builtins.open", side_effect=FileNotFoundError)
    def test_load_config_file_not_found(self, mock_open_file):
        with self.assertRaises(FileNotFoundError):
            setup_config()


class TestValidateHosts(unittest.TestCase):
    def test_valid_as_ip_address_type(self):
        valid_ip = ipaddress.ip_address("192.168.0.1")
        self.assertEqual(validate_host(valid_ip), ipaddress.IPv4Address("192.168.0.1"))

    def test_valid_as_string(self):
        valid_ip = "192.168.0.1"
        self.assertEqual(validate_host(valid_ip), ipaddress.IPv4Address("192.168.0.1"))

    def test_valid_as_ipv4_address_type(self):
        valid_ip = ipaddress.IPv4Address("192.168.0.1")
        self.assertEqual(validate_host(valid_ip), ipaddress.IPv4Address("192.168.0.1"))

    def test_valid_as_ipv6_address_type(self):
        valid_ip = ipaddress.IPv6Address("fe80::1")
        self.assertEqual(validate_host(valid_ip), ipaddress.IPv6Address("fe80::1"))

    def test_invalid_ip(self):
        invalid_ip = "256.256.256.256"
        with self.assertRaises(ValueError):
            validate_host(invalid_ip)

    def test_invalid_address(self):
        invalid_host = "example.com"
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

    def test_invalid_wrong_type(self):
        port = "wrong"
        with self.assertRaises(TypeError):
            # noinspection PyTypeChecker
            validate_port(port)

    def test_invalid_small_port(self):
        small_port = 0
        with self.assertRaises(ValueError):
            validate_port(small_port)

    def test_invalid_large_port(self):
        large_port = 65536
        with self.assertRaises(ValueError):
            validate_port(large_port)


class TestKafkaDeliveryReport(unittest.TestCase):
    @patch('src.base.utils.logger')
    def test_delivery_failed(self, mock_logger):
        mock_error = MagicMock(spec=KafkaError)
        kafka_delivery_report(mock_error, None)
        mock_logger.warning.assert_called_once()

    @patch('src.base.utils.logger')
    def test_delivery_success(self, mock_logger):
        mock_msg = MagicMock(spec=Message)
        kafka_delivery_report(None, mock_msg)
        mock_logger.debug.assert_called_once()


class TestGetFirstPartOfIPv4Address(unittest.TestCase):
    def test_get_first_part_of_ipv4_address_valid_24_bits(self):
        ipv4_address = ipaddress.IPv4Address('192.168.1.1')
        first_part_ipv4 = get_first_part_of_ipv4_address(ipv4_address, 24)
        expected_first_part = ipaddress.IPv4Address('192.168.1.0')
        self.assertEqual(expected_first_part, first_part_ipv4)

    def test_get_first_part_of_ipv4_address_valid_12_bits(self):
        ipv4_address = ipaddress.IPv4Address('192.168.1.1')
        first_part_ipv4 = get_first_part_of_ipv4_address(ipv4_address, 12)
        expected_first_part = ipaddress.IPv4Address('192.160.0.0')
        self.assertEqual(expected_first_part, first_part_ipv4)

    def test_get_first_part_of_ipv4_address_zero_24_bits(self):
        ipv4_address = ipaddress.IPv4Address('0.0.0.1')
        first_part_ipv4 = get_first_part_of_ipv4_address(ipv4_address, 24)
        expected_first_part = ipaddress.IPv4Address('0.0.0.0')
        self.assertEqual(expected_first_part, first_part_ipv4)

    def test_get_first_part_of_ipv4_address_max_24_bits(self):
        ipv4_address = ipaddress.IPv4Address('255.255.255.1')
        first_part_ipv4 = get_first_part_of_ipv4_address(ipv4_address, 24)
        expected_first_part = ipaddress.IPv4Address('255.255.255.0')
        self.assertEqual(expected_first_part, first_part_ipv4)

    def test_get_first_part_of_ipv4_address_invalid_24_bits(self):
        ipv6_address = ipaddress.IPv6Address('2001:0db8:85a3:0000:0000:8a2e:0370:7334')
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            get_first_part_of_ipv4_address(ipv6_address, 24)

    def test_get_first_part_of_ipv4_address_valid(self):
        ipv4_address = ipaddress.IPv4Address('192.168.1.1')
        first_part_ipv4 = get_first_part_of_ipv4_address(ipv4_address, 12)
        expected_first_part = ipaddress.IPv4Address('192.160.0.0')
        self.assertEqual(first_part_ipv4, expected_first_part)

    def test_get_first_part_of_ipv4_address_zero_length(self):
        ipv4_address = ipaddress.IPv4Address('192.168.1.1')
        first_part_ipv4 = get_first_part_of_ipv4_address(ipv4_address, 0)
        expected_first_part = ipaddress.IPv4Address('0.0.0.0')
        self.assertEqual(first_part_ipv4, expected_first_part)

    def test_get_first_part_of_ipv4_address_full_length(self):
        ipv4_address = ipaddress.IPv4Address('192.168.1.1')
        first_part_ipv4 = get_first_part_of_ipv4_address(ipv4_address, 32)
        self.assertEqual(first_part_ipv4, ipv4_address)

    def test_get_first_part_of_ipv4_address_invalid_length(self):
        ipv4_address = ipaddress.IPv4Address('192.168.1.1')
        with self.assertRaises(ValueError):
            get_first_part_of_ipv4_address(ipv4_address, -1)
        with self.assertRaises(ValueError):
            get_first_part_of_ipv4_address(ipv4_address, 33)

    def test_get_first_part_of_ipv4_address_invalid_format(self):
        ipv6_address = ipaddress.IPv6Address('2001:0db8:85a3:0000:0000:8a2e:0370:7334')
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            get_first_part_of_ipv4_address(ipv6_address, 12)


if __name__ == "__main__":
    unittest.main()
