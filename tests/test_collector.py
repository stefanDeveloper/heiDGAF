import ipaddress
import unittest
from ipaddress import IPv4Address, IPv6Address
from unittest.mock import MagicMock, patch

from src.logcollector.collector import LogCollector, main

LOG_SERVER_IP_ADDR = "172.27.0.8"
LOG_SERVER_PORT = 9999


class TestInit(unittest.TestCase):
    @patch("src.logcollector.collector.LOGSERVER_HOSTNAME", "127.0.0.1")
    @patch("src.logcollector.collector.LOGSERVER_SENDING_PORT", 9999)
    @patch("src.logcollector.collector.CollectorKafkaBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    def test_valid_init_ipv4(self, mock_logline_handler, mock_batch_handler):
        mock_batch_handler_instance = MagicMock()
        mock_batch_handler.return_value = mock_batch_handler_instance
        mock_logline_handler_instance = MagicMock()
        mock_logline_handler.return_value = mock_logline_handler_instance

        host = "127.0.0.1"
        port = 9999

        sut = LogCollector()

        self.assertEqual(IPv4Address(host), sut.log_server.get("host"))
        self.assertEqual(port, sut.log_server.get("port"))
        self.assertIsNone(sut.logline)
        self.assertEqual(mock_batch_handler_instance, sut.batch_handler)
        self.assertEqual(mock_logline_handler_instance, sut.logline_handler)

        mock_batch_handler.assert_called_once()
        mock_logline_handler.assert_called_once()

    @patch("src.logcollector.collector.LOGSERVER_HOSTNAME", "fe80::1")
    @patch("src.logcollector.collector.LOGSERVER_SENDING_PORT", 8989)
    @patch("src.logcollector.collector.CollectorKafkaBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    def test_valid_init_ipv6(self, mock_logline_handler, mock_batch_handler):
        mock_batch_handler_instance = MagicMock()
        mock_batch_handler.return_value = mock_batch_handler_instance
        mock_logline_handler_instance = MagicMock()
        mock_logline_handler.return_value = mock_logline_handler_instance

        host = "fe80::1"
        port = 8989

        sut = LogCollector()

        self.assertEqual(IPv6Address(host), sut.log_server.get("host"))
        self.assertEqual(port, sut.log_server.get("port"))
        self.assertIsNone(sut.logline)
        self.assertEqual(mock_batch_handler_instance, sut.batch_handler)
        self.assertEqual(mock_logline_handler_instance, sut.logline_handler)

        mock_batch_handler.assert_called_once()
        mock_logline_handler.assert_called_once()

    @patch("src.logcollector.collector.LOGSERVER_HOSTNAME", "256.256.256.256")
    @patch("src.logcollector.collector.LOGSERVER_SENDING_PORT", 9999)
    def test_invalid_init_with_invalid_host(self):
        with self.assertRaises(ValueError):
            LogCollector()

    @patch("src.logcollector.collector.LOGSERVER_HOSTNAME", "127.0.0.1")
    @patch("src.logcollector.collector.LOGSERVER_SENDING_PORT", 70000)
    def test_invalid_init_with_invalid_port(self):
        with self.assertRaises(ValueError):
            LogCollector()


class TestFetchLogline(unittest.TestCase):
    @patch("src.logcollector.collector.logger")
    @patch("src.logcollector.collector.CollectorKafkaBatchSender")
    @patch("socket.socket")
    def test_fetch_logline_successful(
        self, mock_socket, mock_batch_handler, mock_logger
    ):
        mock_socket_instance = mock_socket.return_value.__enter__.return_value
        mock_socket_instance.connect.return_value = None
        mock_socket_instance.recv.side_effect = ["fake messages".encode("utf-8"), b""]
        mock_batch_handler_instance = MagicMock()
        mock_batch_handler.return_value = mock_batch_handler_instance

        sut = LogCollector()
        sut.fetch_logline()

        mock_socket_instance.connect.assert_called_with(
            (LOG_SERVER_IP_ADDR, LOG_SERVER_PORT)
        )
        mock_socket_instance.recv.assert_called_with(1024)
        self.assertEqual("fake messages", sut.logline)

    @patch("src.logcollector.collector.logger")
    @patch("src.logcollector.collector.CollectorKafkaBatchSender")
    @patch("socket.socket")
    def test_fetch_logline_no_data_on_server(
        self, mock_socket, mock_batch_handler, mock_logger
    ):
        mock_socket_instance = mock_socket.return_value.__enter__.return_value
        mock_socket_instance.connect.return_value = None
        mock_socket_instance.recv.side_effect = ["".encode("utf-8"), b""]
        mock_batch_handler_instance = MagicMock()
        mock_batch_handler.return_value = mock_batch_handler_instance

        sut = LogCollector()
        sut.fetch_logline()

        mock_socket_instance.connect.assert_called_with(
            (LOG_SERVER_IP_ADDR, LOG_SERVER_PORT)
        )
        mock_socket_instance.recv.assert_called_with(1024)
        self.assertIsNone(sut.logline)

    @patch("src.logcollector.collector.logger")
    @patch("src.logcollector.collector.CollectorKafkaBatchSender")
    @patch("socket.socket")
    def test_fetch_logline_connection_error(
        self, mock_socket, mock_batch_handler, mock_logger
    ):
        mock_socket_instance = mock_socket.return_value.__enter__.return_value
        mock_socket_instance.connect.side_effect = ConnectionError("Unable to connect")
        mock_batch_handler_instance = MagicMock()
        mock_batch_handler.return_value = mock_batch_handler_instance

        sut = LogCollector()

        with self.assertRaises(ConnectionError):
            sut.fetch_logline()

        mock_socket_instance.connect.assert_called_with(
            (LOG_SERVER_IP_ADDR, LOG_SERVER_PORT)
        )
        self.assertIsNone(sut.logline)


class TestGetSubnetId(unittest.TestCase):
    @patch("src.logcollector.collector.IPV4_PREFIX_LENGTH", 24)
    @patch("src.logcollector.collector.CollectorKafkaBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    def test_get_subnet_id_ipv4(self, mock_logline_handler, mock_batch_handler):
        # Arrange
        test_address = ipaddress.IPv4Address("192.168.1.1")
        expected_result = f"192.168.1.0_24"
        sut = LogCollector()

        # Act
        result = sut.get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.IPV4_PREFIX_LENGTH", 24)
    @patch("src.logcollector.collector.CollectorKafkaBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    def test_get_subnet_id_ipv4_zero(self, mock_logline_handler, mock_batch_handler):
        # Arrange
        test_address = ipaddress.IPv4Address("0.0.0.0")
        expected_result = f"0.0.0.0_24"
        sut = LogCollector()

        # Act
        result = sut.get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.IPV4_PREFIX_LENGTH", 23)
    @patch("src.logcollector.collector.CollectorKafkaBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    def test_get_subnet_id_ipv4_max(self, mock_logline_handler, mock_batch_handler):
        # Arrange
        test_address = ipaddress.IPv4Address("255.255.255.255")
        expected_result = f"255.255.254.0_23"
        sut = LogCollector()

        # Act
        result = sut.get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.IPV6_PREFIX_LENGTH", 64)
    @patch("src.logcollector.collector.CollectorKafkaBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    def test_get_subnet_id_ipv6(self, mock_logline_handler, mock_batch_handler):
        # Arrange
        test_address = ipaddress.IPv6Address("2001:db8:85a3:1234:5678:8a2e:0370:7334")
        expected_result = f"2001:db8:85a3:1234::_64"
        sut = LogCollector()

        # Act
        result = sut.get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.IPV6_PREFIX_LENGTH", 64)
    @patch("src.logcollector.collector.CollectorKafkaBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    def test_get_subnet_id_ipv6_zero(self, mock_logline_handler, mock_batch_handler):
        # Arrange
        test_address = ipaddress.IPv6Address("::")
        expected_result = f"::_64"
        sut = LogCollector()

        # Act
        result = sut.get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.IPV6_PREFIX_LENGTH", 48)
    @patch("src.logcollector.collector.CollectorKafkaBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    def test_get_subnet_id_ipv6_max(self, mock_logline_handler, mock_batch_handler):
        # Arrange
        test_address = ipaddress.IPv6Address("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff")
        expected_result = f"ffff:ffff:ffff::_48"
        sut = LogCollector()

        # Act
        result = sut.get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.IPV4_PREFIX_LENGTH", 24)
    @patch("src.logcollector.collector.IPV6_PREFIX_LENGTH", 48)
    @patch("src.logcollector.collector.CollectorKafkaBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    def test_get_subnet_id_unsupported_type(
            self, mock_logline_handler, mock_batch_handler
    ):
        # Arrange
        test_address = "192.168.1.1"  # String instead of IPv4Address or IPv6Address
        sut = LogCollector()

        # Act & Assert
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            sut.get_subnet_id(test_address)

    @patch("src.logcollector.collector.IPV4_PREFIX_LENGTH", 24)
    @patch("src.logcollector.collector.IPV6_PREFIX_LENGTH", 48)
    @patch("src.logcollector.collector.CollectorKafkaBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    def test_get_subnet_id_none(self, mock_logline_handler, mock_batch_handler):
        # Arrange
        test_address = None
        sut = LogCollector()

        # Act & Assert
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            sut.get_subnet_id(test_address)


class TestAddLoglineToBatch(unittest.TestCase):
    @patch("src.logcollector.collector.logger")
    @patch("src.logcollector.collector.IPV4_PREFIX_LENGTH", 22)
    @patch("src.base.utils.normalize_ipv4_address")
    @patch("src.logcollector.collector.CollectorKafkaBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    def test_add_to_batch_with_data(
            self, mock_logline_handler, mock_batch_handler, mock_normalize, mock_logger
    ):
        mock_batch_handler_instance = MagicMock()
        mock_logline_handler_instance = MagicMock()
        mock_batch_handler.return_value = mock_batch_handler_instance
        mock_logline_handler.return_value = mock_logline_handler_instance
        mock_logline_handler_instance.validate_logline_and_get_fields_as_json.return_value = {
            "timestamp": "2024-05-21T08:31:28.119Z",
            "status": "NOERROR",
            "client_ip": "192.168.0.105",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.heidelberg-botanik.de",
            "record_type": "A",
            "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
            "size": "150b",
        }
        mock_normalize.return_value = ("192.168.0.0", 22)

        expected_message = (
            '{"timestamp": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": '
            '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": "www.heidelberg-botanik.de", '
            '"record_type": "A", "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", '
            '"size": "150b"}'
        )

        sut = LogCollector()
        sut.logline = (
            "2024-05-21T08:31:28.119Z NOERROR 192.168.0.105 8.8.8.8 www.heidelberg-botanik.de A "
            "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1 150b"
        )
        sut.add_logline_to_batch()

        mock_batch_handler_instance.add_message.assert_called_once_with(
            "192.168.0.0_22", expected_message
        )

    @patch("src.logcollector.collector.CollectorKafkaBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    def test_add_to_batch_without_data(self, mock_logline_handler, mock_batch_handler):
        mock_batch_handler_instance = MagicMock()
        mock_batch_handler.return_value = mock_batch_handler_instance

        sut = LogCollector()
        sut.logline = None

        with self.assertRaises(ValueError):
            sut.add_logline_to_batch()

        mock_batch_handler.add_message.assert_not_called()


class TestClearLogline(unittest.TestCase):
    @patch("src.logcollector.collector.CollectorKafkaBatchSender")
    def test_clear_logline(self, mock_batch_handler):
        mock_batch_handler_instance = MagicMock()
        mock_batch_handler.return_value = mock_batch_handler_instance

        sut = LogCollector()
        sut.logline = (
            "2024-05-21T08:31:28.119Z NOERROR 192.168.0.105 8.8.8.8 "
            "www.heidelberg-botanik.de A b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1 150b"
        )
        sut.clear_logline()

        self.assertIsNone(sut.logline)
        self.assertEqual(IPv4Address(LOG_SERVER_IP_ADDR), sut.log_server["host"])
        self.assertEqual(LOG_SERVER_PORT, sut.log_server["port"])


class TestMainFunction(unittest.TestCase):
    @patch("src.logcollector.collector.logger.info", MagicMock)
    @patch("src.logcollector.collector.LogCollector")
    def test_main_loop_execution(self, mock_log_collector):
        # Arrange
        mock_collector_instance = mock_log_collector.return_value

        mock_collector_instance.fetch_logline = MagicMock()
        mock_collector_instance.add_logline_to_batch = MagicMock()
        mock_collector_instance.clear_logline = MagicMock()

        # Act
        main(one_iteration=True)

        # Assert
        self.assertTrue(mock_collector_instance.fetch_logline.called)
        self.assertTrue(mock_collector_instance.add_logline_to_batch.called)
        self.assertTrue(mock_collector_instance.clear_logline.called)

    @patch("src.logcollector.collector.logger")
    @patch("src.logcollector.collector.LogCollector")
    def test_main_value_error_handling(self, mock_log_collector, mock_logger):
        # Arrange
        mock_collector_instance = mock_log_collector.return_value

        # Act
        with patch.object(
            mock_collector_instance,
            "fetch_logline",
            side_effect=ValueError("Simulated ValueError"),
        ):
            main(one_iteration=True)

        # Assert
        self.assertTrue(mock_collector_instance.clear_logline.called)
        self.assertTrue(mock_collector_instance.loop_exited)

    @patch("src.logcollector.collector.logger")
    @patch("src.logcollector.collector.LogCollector")
    def test_main_keyboard_interrupt(self, mock_log_collector, mock_logger):
        # Arrange
        mock_collector_instance = mock_log_collector.return_value
        mock_collector_instance.fetch_logline.side_effect = KeyboardInterrupt

        # Act
        main()

        # Assert
        self.assertTrue(mock_collector_instance.clear_logline.called)
        self.assertTrue(mock_collector_instance.loop_exited)


if __name__ == "__main__":
    unittest.main()
