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

    @patch("src.base.utils.logger")
    @patch("src.base.utils.CONFIG_FILEPATH", "fake/path/config.yaml")
    @patch("builtins.open", side_effect=FileNotFoundError)
    def test_load_config_file_not_found(self, mock_open_file, mock_logger):
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
    @patch("src.base.utils.logger")
    def test_delivery_failed(self, mock_logger):
        mock_error = MagicMock(spec=KafkaError)
        kafka_delivery_report(mock_error, None)
        mock_logger.warning.assert_called_once()

    @patch("src.base.utils.logger")
    def test_delivery_success(self, mock_logger):
        mock_msg = MagicMock(spec=Message)
        kafka_delivery_report(None, mock_msg)
        mock_logger.debug.assert_called_once()


class TestNormalizeIPv4Address(unittest.TestCase):
    def test_normalize_ip_address_valid_24_bits(self):
        # Arrange
        test_address = ipaddress.IPv4Address("192.168.1.1")
        prefix_length = 24
        expected_result_address = ipaddress.IPv4Address("192.168.1.0")

        # Act
        result = normalize_ipv4_address(test_address, prefix_length)

        # Assert
        expected_result = (expected_result_address, prefix_length)
        self.assertEqual(expected_result, result)

    def test_normalize_ip_address_valid_12_bits(self):
        # Arrange
        test_address = ipaddress.IPv4Address("192.168.1.1")
        prefix_length = 12
        expected_result_address = ipaddress.IPv4Address("192.160.0.0")

        # Act
        result = normalize_ipv4_address(test_address, prefix_length)

        # Assert
        expected_result = (expected_result_address, prefix_length)
        self.assertEqual(expected_result, result)

    def test_normalize_ip_address_zero_24_bits(self):
        # Arrange
        test_address = ipaddress.IPv4Address("0.0.0.1")
        prefix_length = 24
        expected_result_address = ipaddress.IPv4Address("0.0.0.0")

        # Act
        result = normalize_ipv4_address(test_address, prefix_length)

        # Assert
        expected_result = (expected_result_address, prefix_length)
        self.assertEqual(expected_result, result)

    def test_normalize_ip_address_max_23_bits(self):
        # Arrange
        test_address = ipaddress.IPv4Address("255.255.255.1")
        prefix_length = 23
        expected_result_address = ipaddress.IPv4Address("255.255.254.0")

        # Act
        result = normalize_ipv4_address(test_address, prefix_length)

        # Assert
        expected_result = (expected_result_address, prefix_length)
        self.assertEqual(expected_result, result)

    def test_normalize_ip_address_invalid_24_bits(self):
        test_address = ipaddress.IPv6Address("2001:0db8:85a3:0000:0000:8a2e:0370:7334")
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            normalize_ipv4_address(test_address, 24)

    def test_normalize_ip_address_valid(self):
        # Arrange
        test_address = ipaddress.IPv4Address("192.168.1.1")
        prefix_length = 12
        expected_result_address = ipaddress.IPv4Address("192.160.0.0")

        # Act
        result = normalize_ipv4_address(test_address, prefix_length)

        # Assert
        expected_result = (expected_result_address, prefix_length)
        self.assertEqual(expected_result, result)

    def test_normalize_ip_address_zero_length(self):
        # Arrange
        test_address = ipaddress.IPv4Address("192.168.1.1")
        prefix_length = 0
        expected_result_address = ipaddress.IPv4Address("0.0.0.0")

        # Act
        result = normalize_ipv4_address(test_address, prefix_length)

        # Assert
        expected_result = (expected_result_address, prefix_length)
        self.assertEqual(expected_result, result)

    def test_normalize_ip_address_full_length(self):
        # Arrange
        test_address = ipaddress.IPv4Address("192.168.1.1")
        prefix_length = 32
        expected_result_address = ipaddress.IPv4Address("192.168.1.1")

        # Act
        result = normalize_ipv4_address(test_address, prefix_length)

        # Assert
        expected_result = (expected_result_address, prefix_length)
        self.assertEqual(expected_result, result)

    def test_normalize_ip_address_invalid_length(self):
        test_address = ipaddress.IPv4Address("192.168.1.1")
        with self.assertRaises(ValueError):
            normalize_ipv4_address(test_address, -1)
        with self.assertRaises(ValueError):
            normalize_ipv4_address(test_address, 33)

    def test_normalize_ip_address_invalid_format(self):
        test_address = ipaddress.IPv6Address("2001:0db8:85a3:0000:0000:8a2e:0370:7334")
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            normalize_ipv4_address(test_address, 12)


class TestNormalizeIPv6Address(unittest.TestCase):
    def test_normalize_ip_address_valid_64_bits(self):
        # Arrange
        test_address = ipaddress.IPv6Address("2001:db8:85a3:1234:5678:8a2e:0370:7334")
        prefix_length = 64
        expected_result_address = ipaddress.IPv6Address("2001:db8:85a3:1234::")

        # Act
        result = normalize_ipv6_address(test_address, prefix_length)

        # Assert
        expected_result = (expected_result_address, prefix_length)
        self.assertEqual(expected_result, result)

    def test_normalize_ip_address_valid_48_bits(self):
        # Arrange
        test_address = ipaddress.IPv6Address("2001:db8:85a3:1234:5678:8a2e:0370:7334")
        prefix_length = 48
        expected_result_address = ipaddress.IPv6Address("2001:db8:85a3::")

        # Act
        result = normalize_ipv6_address(test_address, prefix_length)

        # Assert
        expected_result = (expected_result_address, prefix_length)
        self.assertEqual(expected_result, result)

    def test_normalize_ip_address_zero_64_bits(self):
        # Arrange
        test_address = ipaddress.IPv6Address("::1")
        prefix_length = 64
        expected_result_address = ipaddress.IPv6Address("::")

        # Act
        result = normalize_ipv6_address(test_address, prefix_length)

        # Assert
        expected_result = (expected_result_address, prefix_length)
        self.assertEqual(expected_result, result)

    def test_normalize_ip_address_max_63_bits(self):
        # Arrange
        test_address = ipaddress.IPv6Address("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff")
        prefix_length = 63
        expected_result_address = ipaddress.IPv6Address("ffff:ffff:ffff:fffe::")

        # Act
        result = normalize_ipv6_address(test_address, prefix_length)

        # Assert
        expected_result = (expected_result_address, prefix_length)
        self.assertEqual(expected_result, result)

    def test_normalize_ip_address_invalid_64_bits(self):
        test_address = ipaddress.IPv4Address("192.168.1.1")
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            normalize_ipv6_address(test_address, 64)

    def test_normalize_ip_address_valid(self):
        # Arrange
        test_address = ipaddress.IPv6Address("2001:db8:85a3:1234:5678:8a2e:0370:7334")
        prefix_length = 32
        expected_result_address = ipaddress.IPv6Address("2001:db8::")

        # Act
        result = normalize_ipv6_address(test_address, prefix_length)

        # Assert
        expected_result = (expected_result_address, prefix_length)
        self.assertEqual(expected_result, result)

    def test_normalize_ip_address_zero_length(self):
        # Arrange
        test_address = ipaddress.IPv6Address("2001:db8:85a3:1234:5678:8a2e:0370:7334")
        prefix_length = 0
        expected_result_address = ipaddress.IPv6Address("::")

        # Act
        result = normalize_ipv6_address(test_address, prefix_length)

        # Assert
        expected_result = (expected_result_address, prefix_length)
        self.assertEqual(expected_result, result)

    def test_normalize_ip_address_full_length(self):
        # Arrange
        test_address = ipaddress.IPv6Address("2001:db8:85a3:1234:5678:8a2e:0370:7334")
        prefix_length = 128
        expected_result_address = ipaddress.IPv6Address(
            "2001:db8:85a3:1234:5678:8a2e:0370:7334"
        )

        # Act
        result = normalize_ipv6_address(test_address, prefix_length)

        # Assert
        expected_result = (expected_result_address, prefix_length)
        self.assertEqual(expected_result, result)

    def test_normalize_ip_address_invalid_length(self):
        test_address = ipaddress.IPv6Address("2001:db8:85a3:1234:5678:8a2e:0370:7334")
        with self.assertRaises(ValueError):
            normalize_ipv6_address(test_address, -1)
        with self.assertRaises(ValueError):
            normalize_ipv6_address(test_address, 129)

    def test_normalize_ip_address_invalid_format(self):
        test_address = ipaddress.IPv4Address("192.168.1.1")
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            normalize_ipv6_address(test_address, 64)

class TestGetBatchConfiguration(unittest.TestCase):
    @patch("src.base.utils.setup_config")
    def test_no_matching_collector(self, mock_setup_config):
        # Arrange
        mock_config = {
            "pipeline": {
                "log_collection": {
                    "default_batch_handler_config": {"batch_size": 100, "batch_timeout": 5.0},
                    "collectors": [
                        {"name": "collector1", "protocol_base": "dns"},
                        {"name": "collector2", "protocol_base": "http"}
                    ]
                }
            }
        }
        mock_setup_config.return_value = mock_config
        
        # Act
        result = get_batch_configuration("non_existent_collector")
        
        # Assert
        expected = {"batch_size": 100, "batch_timeout": 5.0}
        self.assertEqual(result, expected)

    @patch("src.base.utils.setup_config")
    def test_matching_collector_no_override(self, mock_setup_config):
        # Arrange
        mock_config = {
            "pipeline": {
                "log_collection": {
                    "default_batch_handler_config": {"batch_size": 100, "batch_timeout": 5.0},
                    "collectors": [
                        {"name": "test_collector", "protocol_base": "dns"}
                    ]
                }
            }
        }
        mock_setup_config.return_value = mock_config
        
        # Act
        result = get_batch_configuration("test_collector")
        
        # Assert
        expected = {"batch_size": 100, "batch_timeout": 5.0}
        self.assertEqual(result, expected)

    @patch("src.base.utils.setup_config")
    def test_matching_collector_with_full_override(self, mock_setup_config):
        # Arrange
        mock_config = {
            "pipeline": {
                "log_collection": {
                    "default_batch_handler_config": {"batch_size": 100, "batch_timeout": 5.0},
                    "collectors": [
                        {
                            "name": "test_collector",
                            "protocol_base": "dns",
                            "batch_handler_config_override": {"batch_size": 200, "batch_timeout": 10.0}
                        }
                    ]
                }
            }
        }
        mock_setup_config.return_value = mock_config
        
        # Act
        result = get_batch_configuration("test_collector")
        
        # Assert
        expected = {"batch_size": 200, "batch_timeout": 10.0}
        self.assertEqual(result, expected)

    @patch("src.base.utils.setup_config")
    def test_matching_collector_with_partial_override(self, mock_setup_config):
        # Arrange
        mock_config = {
            "pipeline": {
                "log_collection": {
                    "default_batch_handler_config": {"batch_size": 100, "batch_timeout": 5.0},
                    "collectors": [
                        {
                            "name": "test_collector",
                            "protocol_base": "dns",
                            "batch_handler_config_override": {"batch_size": 200}
                        }
                    ]
                }
            }
        }
        mock_setup_config.return_value = mock_config
        
        # Act
        result = get_batch_configuration("test_collector")
        
        # Assert
        expected = {"batch_size": 200, "batch_timeout": 5.0}
        self.assertEqual(result, expected)

    @patch("src.base.utils.setup_config")
    def test_multiple_collectors_with_same_name_first_has_override(self, mock_setup_config):
        # Arrange
        mock_config = {
            "pipeline": {
                "log_collection": {
                    "default_batch_handler_config": {"batch_size": 100, "batch_timeout": 5.0},
                    "collectors": [
                        {
                            "name": "test_collector",
                            "protocol_base": "dns",
                            "batch_handler_config_override": {"batch_size": 200}
                        },
                        {"name": "test_collector", "protocol_base": "http"}
                    ]
                }
            }
        }
        mock_setup_config.return_value = mock_config
        
        # Act
        result = get_batch_configuration("test_collector")
        
        # Assert - should return the first match, which has an override
        expected = {"batch_size": 200, "batch_timeout": 5.0}
        self.assertEqual(result, expected)

if __name__ == "__main__":
    unittest.main()
