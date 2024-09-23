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
    @patch("src.logcollector.collector.CollectorKafkaBatchSender")
    @patch("socket.socket")
    def test_fetch_logline_successful(self, mock_socket, mock_batch_handler):
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

    @patch("src.logcollector.collector.CollectorKafkaBatchSender")
    @patch("socket.socket")
    def test_fetch_logline_no_data_on_server(self, mock_socket, mock_batch_handler):
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

    @patch("src.logcollector.collector.CollectorKafkaBatchSender")
    @patch("socket.socket")
    def test_fetch_logline_connection_error(self, mock_socket, mock_batch_handler):
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


# TODO: Maybe use for logline handler tests
# class TestValidateAndExtractLogline(unittest.TestCase):
#     @patch("src.logcollector.collector.CollectorKafkaBatchSender")
#     def test_valid_logline(self, mock_batch_handler):
#         mock_batch_handler_instance = MagicMock()
#         mock_batch_handler.return_value = mock_batch_handler_instance
#
#         sut = LogCollector()
#         sut.logline = (
#             "2024-05-21T19:27:15.583Z NOERROR 192.168.0.253 8.8.8.8 www.uni-hd-theologie.de "
#             "A b49c:50f9:a37:f8e2:ff81:8be7:3e88:d27d 86b"
#         )
#         sut.validate_and_extract_logline()
#
#         self.assertEqual("2024-05-21T19:27:15.583Z", sut.log_data.get("timestamp"))
#         self.assertEqual("NOERROR", sut.log_data.get("status"))
#         self.assertEqual(IPv4Address("192.168.0.253"), sut.log_data.get("client_ip"))
#         self.assertEqual(IPv4Address("8.8.8.8"), sut.log_data.get("dns_ip"))
#         self.assertEqual(
#             "www.uni-hd-theologie.de", sut.log_data.get("host_domain_name")
#         )
#         self.assertEqual("A", sut.log_data.get("record_type"))
#         self.assertEqual(
#             IPv6Address("b49c:50f9:a37:f8e2:ff81:8be7:3e88:d27d"),
#             sut.log_data.get("response_ip"),
#         )
#         self.assertEqual("86b", sut.log_data.get("size"))
#
#     @patch("src.logcollector.collector.CollectorKafkaBatchSender")
#     def test_invalid_logline_no_data(self, mock_batch_handler):
#         mock_batch_handler_instance = MagicMock()
#         mock_batch_handler.return_value = mock_batch_handler_instance
#
#         sut = LogCollector()
#         sut.logline = None
#
#         with self.assertRaises(ValueError):
#             sut.validate_and_extract_logline()
#
#     @patch("src.logcollector.collector.CollectorKafkaBatchSender")
#     def test_invalid_logline_wrong_length(self, mock_batch_handler):
#         mock_batch_handler_instance = MagicMock()
#         mock_batch_handler.return_value = mock_batch_handler_instance
#
#         sut = LogCollector()
#         sut.logline = (
#             "2024-05-21T19:27:15.583Z NOERROR 192.168.0.253 8.8.8.8 www.uni-hd-theologie.de "
#             "A b49c:50f9:a37:f8e2:ff81:8be7:3e88:d27d 86b THIS_IS_WRONG"
#         )
#
#         with self.assertRaises(ValueError):
#             sut.validate_and_extract_logline()
#
#         self.assertIsNone(sut.log_data.get("timestamp"))
#         self.assertIsNone(sut.log_data.get("status"))
#         self.assertIsNone(sut.log_data.get("client_ip"))
#         self.assertIsNone(sut.log_data.get("dns_ip"))
#         self.assertIsNone(sut.log_data.get("host_domain_name"))
#         self.assertIsNone(sut.log_data.get("record_type"))
#         self.assertIsNone(sut.log_data.get("response_ip"))
#         self.assertIsNone(sut.log_data.get("size"))
#
#     @patch("src.logcollector.collector.CollectorKafkaBatchSender")
#     def test_invalid_logline_wrong_timestamp(self, mock_batch_handler):
#         mock_batch_handler_instance = MagicMock()
#         mock_batch_handler.return_value = mock_batch_handler_instance
#
#         sut = LogCollector()
#         sut.logline = (
#             "wrong_timestamp NOERROR 192.168.0.253 8.8.8.8 www.uni-hd-theologie.de "
#             "A b49c:50f9:a37:f8e2:ff81:8be7:3e88:d27d 86b"
#         )
#
#         with self.assertRaises(ValueError):
#             sut.validate_and_extract_logline()
#
#         self.assertIsNone(sut.log_data.get("timestamp"))
#         self.assertIsNone(sut.log_data.get("status"))
#         self.assertIsNone(sut.log_data.get("client_ip"))
#         self.assertIsNone(sut.log_data.get("dns_ip"))
#         self.assertIsNone(sut.log_data.get("host_domain_name"))
#         self.assertIsNone(sut.log_data.get("record_type"))
#         self.assertIsNone(sut.log_data.get("response_ip"))
#         self.assertIsNone(sut.log_data.get("size"))
#
#     @patch("src.logcollector.collector.CollectorKafkaBatchSender")
#     def test_invalid_logline_wrong_status(self, mock_batch_handler):
#         mock_batch_handler_instance = MagicMock()
#         mock_batch_handler.return_value = mock_batch_handler_instance
#
#         sut = LogCollector()
#         sut.logline = (
#             "2024-05-21T19:27:15.583Z WRONG_STATUS 192.168.0.253 8.8.8.8 www.uni-hd-theologie.de "
#             "A b49c:50f9:a37:f8e2:ff81:8be7:3e88:d27d 86b"
#         )
#
#         with self.assertRaises(ValueError):
#             sut.validate_and_extract_logline()
#
#         self.assertIsNone(sut.log_data.get("timestamp"))
#         self.assertIsNone(sut.log_data.get("status"))
#         self.assertIsNone(sut.log_data.get("client_ip"))
#         self.assertIsNone(sut.log_data.get("dns_ip"))
#         self.assertIsNone(sut.log_data.get("host_domain_name"))
#         self.assertIsNone(sut.log_data.get("record_type"))
#         self.assertIsNone(sut.log_data.get("response_ip"))
#         self.assertIsNone(sut.log_data.get("size"))
#
#     @patch("src.logcollector.collector.CollectorKafkaBatchSender")
#     def test_invalid_logline_wrong_client_ip(self, mock_batch_handler):
#         mock_batch_handler_instance = MagicMock()
#         mock_batch_handler.return_value = mock_batch_handler_instance
#
#         sut = LogCollector()
#         sut.logline = (
#             "2024-05-21T19:27:15.583Z NOERROR wrong_client_ip 8.8.8.8 www.uni-hd-theologie.de "
#             "A b49c:50f9:a37:f8e2:ff81:8be7:3e88:d27d 86b"
#         )
#
#         with self.assertRaises(ValueError):
#             sut.validate_and_extract_logline()
#
#         self.assertIsNone(sut.log_data.get("timestamp"))
#         self.assertIsNone(sut.log_data.get("status"))
#         self.assertIsNone(sut.log_data.get("client_ip"))
#         self.assertIsNone(sut.log_data.get("dns_ip"))
#         self.assertIsNone(sut.log_data.get("host_domain_name"))
#         self.assertIsNone(sut.log_data.get("record_type"))
#         self.assertIsNone(sut.log_data.get("response_ip"))
#         self.assertIsNone(sut.log_data.get("size"))
#
#     @patch("src.logcollector.collector.CollectorKafkaBatchSender")
#     def test_invalid_logline_wrong_dns_ip(self, mock_batch_handler):
#         mock_batch_handler_instance = MagicMock()
#         mock_batch_handler.return_value = mock_batch_handler_instance
#
#         sut = LogCollector()
#         sut.logline = (
#             "2024-05-21T19:27:15.583Z NOERROR 192.168.0.253 wrong_dns_ip www.uni-hd-theologie.de "
#             "A b49c:50f9:a37:f8e2:ff81:8be7:3e88:d27d 86b"
#         )
#
#         with self.assertRaises(ValueError):
#             sut.validate_and_extract_logline()
#
#         self.assertIsNone(sut.log_data.get("timestamp"))
#         self.assertIsNone(sut.log_data.get("status"))
#         self.assertIsNone(sut.log_data.get("client_ip"))
#         self.assertIsNone(sut.log_data.get("dns_ip"))
#         self.assertIsNone(sut.log_data.get("host_domain_name"))
#         self.assertIsNone(sut.log_data.get("record_type"))
#         self.assertIsNone(sut.log_data.get("response_ip"))
#         self.assertIsNone(sut.log_data.get("size"))
#
#     @patch("src.logcollector.collector.CollectorKafkaBatchSender")
#     def test_invalid_logline_wrong_host_domain_name(self, mock_batch_handler):
#         mock_batch_handler_instance = MagicMock()
#         mock_batch_handler.return_value = mock_batch_handler_instance
#
#         sut = LogCollector()
#         sut.logline = (
#             "2024-05-21T19:27:15.583Z NOERROR 192.168.0.253 8.8.8.8 wrong_host_domain_name "
#             "A b49c:50f9:a37:f8e2:ff81:8be7:3e88:d27d 86b"
#         )
#
#         with self.assertRaises(ValueError):
#             sut.validate_and_extract_logline()
#
#         self.assertIsNone(sut.log_data.get("timestamp"))
#         self.assertIsNone(sut.log_data.get("status"))
#         self.assertIsNone(sut.log_data.get("client_ip"))
#         self.assertIsNone(sut.log_data.get("dns_ip"))
#         self.assertIsNone(sut.log_data.get("host_domain_name"))
#         self.assertIsNone(sut.log_data.get("record_type"))
#         self.assertIsNone(sut.log_data.get("response_ip"))
#         self.assertIsNone(sut.log_data.get("size"))
#
#     @patch("src.logcollector.collector.CollectorKafkaBatchSender")
#     def test_invalid_logline_wrong_record_type(self, mock_batch_handler):
#         mock_batch_handler_instance = MagicMock()
#         mock_batch_handler.return_value = mock_batch_handler_instance
#
#         sut = LogCollector()
#         sut.logline = (
#             "2024-05-21T19:27:15.583Z NOERROR 192.168.0.253 8.8.8.8 www.uni-hd-theologie.de "
#             "WRONG_RECORD_TYPE b49c:50f9:a37:f8e2:ff81:8be7:3e88:d27d 86b"
#         )
#
#         with self.assertRaises(ValueError):
#             sut.validate_and_extract_logline()
#
#         self.assertIsNone(sut.log_data.get("timestamp"))
#         self.assertIsNone(sut.log_data.get("status"))
#         self.assertIsNone(sut.log_data.get("client_ip"))
#         self.assertIsNone(sut.log_data.get("dns_ip"))
#         self.assertIsNone(sut.log_data.get("host_domain_name"))
#         self.assertIsNone(sut.log_data.get("record_type"))
#         self.assertIsNone(sut.log_data.get("response_ip"))
#         self.assertIsNone(sut.log_data.get("size"))
#
#     @patch("src.logcollector.collector.CollectorKafkaBatchSender")
#     def test_invalid_logline_wrong_response_ip(self, mock_batch_handler):
#         mock_batch_handler_instance = MagicMock()
#         mock_batch_handler.return_value = mock_batch_handler_instance
#
#         sut = LogCollector()
#         sut.logline = (
#             "2024-05-21T19:27:15.583Z NOERROR 192.168.0.253 8.8.8.8 www.uni-hd-theologie.de "
#             "A wrong_response_ip 86b"
#         )
#
#         with self.assertRaises(ValueError):
#             sut.validate_and_extract_logline()
#
#         self.assertIsNone(sut.log_data.get("timestamp"))
#         self.assertIsNone(sut.log_data.get("status"))
#         self.assertIsNone(sut.log_data.get("client_ip"))
#         self.assertIsNone(sut.log_data.get("dns_ip"))
#         self.assertIsNone(sut.log_data.get("host_domain_name"))
#         self.assertIsNone(sut.log_data.get("record_type"))
#         self.assertIsNone(sut.log_data.get("response_ip"))
#         self.assertIsNone(sut.log_data.get("size"))
#
#     @patch("src.logcollector.collector.CollectorKafkaBatchSender")
#     def test_invalid_logline_wrong_size(self, mock_batch_handler):
#         mock_batch_handler_instance = MagicMock()
#         mock_batch_handler.return_value = mock_batch_handler_instance
#
#         sut = LogCollector()
#         sut.logline = (
#             "2024-05-21T19:27:15.583Z NOERROR 192.168.0.253 8.8.8.8 www.uni-hd-theologie.de "
#             "A b49c:50f9:a37:f8e2:ff81:8be7:3e88:d27d wrong_size"
#         )
#
#         with self.assertRaises(ValueError):
#             sut.validate_and_extract_logline()
#
#         self.assertIsNone(sut.log_data.get("timestamp"))
#         self.assertIsNone(sut.log_data.get("status"))
#         self.assertIsNone(sut.log_data.get("client_ip"))
#         self.assertIsNone(sut.log_data.get("dns_ip"))
#         self.assertIsNone(sut.log_data.get("host_domain_name"))
#         self.assertIsNone(sut.log_data.get("record_type"))
#         self.assertIsNone(sut.log_data.get("response_ip"))
#         self.assertIsNone(sut.log_data.get("size"))


class TestAddLoglineToBatch(unittest.TestCase):
    @patch("src.logcollector.collector.SUBNET_BITS", 22)
    @patch("src.base.utils.get_first_part_of_ipv4_address")
    @patch("src.logcollector.collector.CollectorKafkaBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    def test_add_to_batch_with_data(self, mock_logline_handler, mock_batch_handler, mock_get):
        mock_batch_handler_instance = MagicMock()
        mock_logline_handler_instance = MagicMock()
        mock_batch_handler.return_value = mock_batch_handler_instance
        mock_logline_handler.return_value = mock_logline_handler_instance
        mock_logline_handler_instance.validate_logline_and_get_fields_as_json.return_value = {
            "timestamp": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.105",
            "dns_ip": "8.8.8.8", "host_domain_name": "www.heidelberg-botanik.de", "record_type": "A",
            "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}
        mock_get.return_value = "192.168.0.0"

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


# TODO: Maybe use for logline handler tests
# class TestCheckLength(unittest.TestCase):
#     def test_valid_length(self):
#         # Valid length of 8
#         parts = ["1", "2", "3", "4", "5", "6", "7", "8"]
#         self.assertTrue(LogCollector._check_length(parts))
#
#     def test_invalid_length_too_small(self):
#         # Invalid length: Length less than 8
#         parts = ["1", "2", "3", "4", "5", "6", "7"]
#         self.assertFalse(LogCollector._check_length(parts))
#
#     def test_invalid_length_too_large(self):
#         # Invalid length: Length more than 8
#         parts = ["1", "2", "3", "4", "5", "6", "7", "8", "9"]
#         self.assertFalse(LogCollector._check_length(parts))
#
#     def test_invalid_length_zero(self):
#         # Invalid length: Length is 0
#         parts = []
#         self.assertFalse(LogCollector._check_length(parts))


# TODO: Maybe use for logline handler tests
# class TestCheckTimestamp(unittest.TestCase):
#     def test_valid_timestamp(self):
#         # Valid timestamp
#         self.assertTrue(LogCollector._check_timestamp("2024-05-21T19:29:05.466Z"))
#
#     def test_invalid_timestamp_wrong_format(self):
#         # Invalid timestamp - Wrong format
#         self.assertFalse(LogCollector._check_timestamp("2023/05/21 15:30:45.123Z"))
#
#     def test_invalid_timestamp_missing_milliseconds(self):
#         # Invalid timestamp - Missing milliseconds
#         self.assertFalse(LogCollector._check_timestamp("2023-05-21T15:30:45Z"))
#
#     def test_invalid_timestamp_missing_Z(self):
#         # Invalid timestamp - Missing 'Z'
#         self.assertFalse(LogCollector._check_timestamp("2023-05-21T15:30:45.123"))
#
#     def test_invalid_timestamp_incomplete(self):
#         # Invalid timestamp - Incomplete timestamp
#         self.assertFalse(LogCollector._check_timestamp("2023-05-21T15:30"))
#
#     def test_invalid_timestamp_non_numeric(self):
#         # Invalid timestamp - Non-numeric value
#         self.assertFalse(LogCollector._check_timestamp("abcd-ef-ghTij:kl:mn.opqZ"))


# TODO: Maybe use for logline handler tests
# class TestCheckStatus(unittest.TestCase):
#     def test_valid_status_noerror(self):
#         # Valid status: NOERROR
#         self.assertTrue(LogCollector._check_status("NOERROR"))
#
#     def test_valid_status_nxdomain(self):
#         # Valid status: NXDOMAIN
#         self.assertTrue(LogCollector._check_status("NXDOMAIN"))
#
#     def test_invalid_status_lowercase(self):
#         # Invalid status: Correct status but lowercase
#         self.assertFalse(LogCollector._check_status("noerror"))
#
#     def test_invalid_status(self):
#         # Invalid status: Unknown status
#         self.assertFalse(LogCollector._check_status("WRONG"))


# TODO: Maybe use for logline handler tests
# class TestCheckDomainName(unittest.TestCase):
#     def test_valid_domain(self):
#         # Valid domain name
#         self.assertTrue(LogCollector._check_domain_name("example.com"))
#
#     def test_valid_subdomain(self):
#         # Valid subdomain
#         self.assertTrue(LogCollector._check_domain_name("sub.example.com"))
#
#     def test_invalid_domain_with_scheme(self):
#         # Invalid domain with scheme
#         self.assertFalse(LogCollector._check_domain_name("http://example.com"))
#
#     def test_invalid_domain_with_invalid_chars(self):
#         # Invalid domain with invalid characters
#         self.assertFalse(LogCollector._check_domain_name("example$.com"))
#
#     def test_invalid_domain_too_long(self):
#         # Invalid domain name too long
#         self.assertFalse(LogCollector._check_domain_name("a" * 256 + ".com"))
#
#     def test_invalid_domain_too_short(self):
#         # Invalid domain name: none
#         self.assertFalse(LogCollector._check_domain_name(".com"))
#
#     def test_invalid_domain_start_with_dash(self):
#         # Invalid domain name starts with a dash
#         self.assertFalse(LogCollector._check_domain_name("-example.com"))
#
#     def test_invalid_domain_end_with_dash(self):
#         # Invalid domain name ends with a dash
#         self.assertFalse(LogCollector._check_domain_name("example-.com"))


# TODO: Maybe use for logline handler tests
# class TestCheckRecordType(unittest.TestCase):
#     def test_valid_record_type_a(self):
#         # Valid status: NOERROR
#         self.assertTrue(LogCollector._check_record_type("A"))
#
#     def test_valid_record_type_aaaa(self):
#         # Valid status: NXDOMAIN
#         self.assertTrue(LogCollector._check_record_type("AAAA"))
#
#     def test_invalid_record_type(self):
#         # Invalid status: Unknown status
#         self.assertFalse(LogCollector._check_record_type("WRONG"))


# TODO: Maybe use for logline handler tests
# class TestCheckSize(unittest.TestCase):
#     def test_valid_size(self):
#         # Valid size
#         self.assertTrue(LogCollector._check_size("50b"))
#
#     def test_valid_size_multiple_digits(self):
#         # Valid size with multiple digits
#         self.assertTrue(LogCollector._check_size("100b"))
#
#     def test_valid_size_large_number(self):
#         # Valid size - Large number
#         self.assertTrue(LogCollector._check_size("10000b"))
#
#     def test_invalid_size_wrong_unit(self):
#         # Invalid size - Unit not allowed
#         self.assertFalse(LogCollector._check_size("100MB"))
#
#     def test_invalid_size_missing_unit(self):
#         # Invalid size - Unit missing
#         self.assertFalse(LogCollector._check_size("100"))
#
#     def test_invalid_size_non_numeric(self):
#         # Invalid size - Non-numeric value
#         self.assertFalse(LogCollector._check_size("abc"))


class TestMainFunction(unittest.TestCase):
    @patch('src.logcollector.collector.LogCollector')
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

    @patch('src.logcollector.collector.LogCollector')
    def test_main_value_error_handling(self, mock_log_collector):
        # Arrange
        mock_collector_instance = mock_log_collector.return_value

        # Act
        with patch.object(mock_collector_instance, 'fetch_logline',
                          side_effect=ValueError('Simulated ValueError')):
            main(one_iteration=True)

        # Assert
        self.assertTrue(mock_collector_instance.clear_logline.called)
        self.assertTrue(mock_collector_instance.loop_exited)

    @patch('src.logcollector.collector.LogCollector')
    def test_main_keyboard_interrupt(self, mock_log_collector):
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
