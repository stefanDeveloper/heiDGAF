import unittest
from ipaddress import IPv4Address, IPv6Address
from unittest.mock import patch

from pipeline_prototype.heidgaf_log_collector.collector import LogCollector, KAFKA_BROKER_HOST, KAFKA_BROKER_PORT


class TestInit(unittest.TestCase):
    @patch('pipeline_prototype.heidgaf_log_collector.collector.Producer')
    def test_valid_init_ipv4(self, mock_producer):
        host = "192.168.0.1"
        port = 9999
        collector_instance = LogCollector(host, port)
        expected_conf = {'bootstrap.servers': f'{KAFKA_BROKER_HOST}:{KAFKA_BROKER_PORT}'}

        mock_producer.assert_called_once_with(expected_conf)
        self.assertIsNotNone(collector_instance.kafka_producer)
        self.assertEqual(IPv4Address(host), collector_instance.log_server.get("host"))
        self.assertEqual(port, collector_instance.log_server.get("port"))
        self.assertEqual(None, collector_instance.logline)
        self.assertEqual(None, collector_instance.log_data.get("timestamp"))
        self.assertEqual(None, collector_instance.log_data.get("status"))
        self.assertEqual(None, collector_instance.log_data.get("client_ip"))
        self.assertEqual(None, collector_instance.log_data.get("dns_ip"))
        self.assertEqual(None, collector_instance.log_data.get("host_domain_name"))
        self.assertEqual(None, collector_instance.log_data.get("record_type"))
        self.assertEqual(None, collector_instance.log_data.get("response_ip"))
        self.assertEqual(None, collector_instance.log_data.get("size"))

    @patch('pipeline_prototype.heidgaf_log_collector.collector.Producer')
    def test_valid_init_ipv6(self, mock_producer):
        host = "fe80::1"
        port = 9999
        collector_instance = LogCollector(host, port)
        expected_conf = {'bootstrap.servers': f'{KAFKA_BROKER_HOST}:{KAFKA_BROKER_PORT}'}

        mock_producer.assert_called_once_with(expected_conf)
        self.assertIsNotNone(collector_instance.kafka_producer)
        self.assertEqual(IPv6Address(host), collector_instance.log_server.get("host"))
        self.assertEqual(port, collector_instance.log_server.get("port"))
        self.assertEqual(None, collector_instance.logline)
        self.assertEqual(None, collector_instance.log_data.get("timestamp"))
        self.assertEqual(None, collector_instance.log_data.get("status"))
        self.assertEqual(None, collector_instance.log_data.get("client_ip"))
        self.assertEqual(None, collector_instance.log_data.get("dns_ip"))
        self.assertEqual(None, collector_instance.log_data.get("host_domain_name"))
        self.assertEqual(None, collector_instance.log_data.get("record_type"))
        self.assertEqual(None, collector_instance.log_data.get("response_ip"))
        self.assertEqual(None, collector_instance.log_data.get("size"))

    def test_invalid_init_with_no_host(self):
        with self.assertRaises(TypeError):
            # noinspection PyArgumentList
            LogCollector(server_port=9999)

    def test_invalid_init_with_no_port(self):
        with self.assertRaises(TypeError):
            # noinspection PyArgumentList
            LogCollector(server_host="192.168.2.1")

    def test_invalid_init_with_invalid_host(self):
        with self.assertRaises(ValueError):
            LogCollector("256.256.256.256", 9999)

    def test_invalid_init_with_invalid_port(self):
        with self.assertRaises(ValueError):
            LogCollector("192.168.0.1", 70000)


class TestFetchLogline(unittest.TestCase):
    @patch('socket.socket')
    def test_fetch_logline(self, mock_socket):
        mock_socket_instance = mock_socket.return_value.__enter__.return_value
        mock_socket_instance.connect.return_value = None
        mock_socket_instance.recv.side_effect = ["fake data".encode('utf-8'), b""]

        host = "127.0.0.1"
        port = 12345

        collector_instance = LogCollector(host, port)
        collector_instance.fetch_logline()

        mock_socket_instance.connect.assert_called_with((host, port))
        mock_socket_instance.recv.assert_called_with(1024)
        self.assertEqual("fake data", collector_instance.logline)


class TestValidateAndExtractLogline(unittest.TestCase):
    def test_valid_logline(self):
        # Valid logline (randomly generated)
        collector_instance = LogCollector("127.0.0.1", 9999)
        collector_instance.logline = ("2024-05-21T19:27:15.583Z NOERROR 192.168.0.253 8.8.8.8 www.uni-hd-theologie.de "
                                      "A b49c:50f9:a37:f8e2:ff81:8be7:3e88:d27d 86b")
        collector_instance.validate_and_extract_logline()
        self.assertEqual("2024-05-21T19:27:15.583Z", collector_instance.log_data.get("timestamp"))
        self.assertEqual("NOERROR", collector_instance.log_data.get("status"))
        self.assertEqual(IPv4Address("192.168.0.253"), collector_instance.log_data.get("client_ip"))
        self.assertEqual(IPv4Address("8.8.8.8"), collector_instance.log_data.get("dns_ip"))
        self.assertEqual("www.uni-hd-theologie.de", collector_instance.log_data.get("host_domain_name"))
        self.assertEqual("A", collector_instance.log_data.get("record_type"))
        self.assertEqual(IPv6Address("b49c:50f9:a37:f8e2:ff81:8be7:3e88:d27d"),
                         collector_instance.log_data.get("response_ip"))
        self.assertEqual("86b", collector_instance.log_data.get("size"))

    def test_invalid_logline_wrong_response_ip(self):
        # Invalid logline: Incorrect Response IP
        collector_instance = LogCollector("127.0.0.1", 9999)
        collector_instance.logline = ("2024-05-21T19:27:15.583Z NOERROR 192.168.0.253 8.8.8.8 www.uni-hd-theologie.de "
                                      "A b49c:50f9:a37:f8e2:ff81:3e88:d27d 86b")

        with self.assertRaises(ValueError):
            collector_instance.validate_and_extract_logline()

        self.assertEqual(None, collector_instance.log_data.get("timestamp"))
        self.assertEqual(None, collector_instance.log_data.get("status"))
        self.assertEqual(None, collector_instance.log_data.get("client_ip"))
        self.assertEqual(None, collector_instance.log_data.get("dns_ip"))
        self.assertEqual(None, collector_instance.log_data.get("host_domain_name"))
        self.assertEqual(None, collector_instance.log_data.get("record_type"))
        self.assertEqual(None, collector_instance.log_data.get("response_ip"))
        self.assertEqual(None, collector_instance.log_data.get("size"))


class TestCheckLength(unittest.TestCase):
    def test_valid_length(self):
        # Valid length of 8
        parts = ["1", "2", "3", "4", "5", "6", "7", "8"]
        self.assertTrue(LogCollector._check_length(parts))

    def test_invalid_length_too_small(self):
        # Invalid length: Length less than 8
        parts = ["1", "2", "3", "4", "5", "6", "7"]
        self.assertFalse(LogCollector._check_length(parts))

    def test_invalid_length_too_large(self):
        # Invalid length: Length more than 8
        parts = ["1", "2", "3", "4", "5", "6", "7", "8", "9"]
        self.assertFalse(LogCollector._check_length(parts))

    def test_invalid_length_zero(self):
        # Invalid length: Length is 0
        parts = []
        self.assertFalse(LogCollector._check_length(parts))


class TestCheckTimestamp(unittest.TestCase):
    def test_valid_timestamp(self):
        # Valid timestamp
        self.assertTrue(LogCollector._check_timestamp("2024-05-21T19:29:05.466Z"))

    def test_invalid_timestamp_wrong_format(self):
        # Invalid timestamp - Wrong format
        self.assertFalse(LogCollector._check_timestamp("2023/05/21 15:30:45.123Z"))

    def test_invalid_timestamp_missing_milliseconds(self):
        # Invalid timestamp - Missing milliseconds
        self.assertFalse(LogCollector._check_timestamp("2023-05-21T15:30:45Z"))

    def test_invalid_timestamp_missing_Z(self):
        # Invalid timestamp - Missing 'Z'
        self.assertFalse(LogCollector._check_timestamp("2023-05-21T15:30:45.123"))

    def test_invalid_timestamp_incomplete(self):
        # Invalid timestamp - Incomplete timestamp
        self.assertFalse(LogCollector._check_timestamp("2023-05-21T15:30"))

    def test_invalid_timestamp_non_numeric(self):
        # Invalid timestamp - Non-numeric value
        self.assertFalse(LogCollector._check_timestamp("abcd-ef-ghTij:kl:mn.opqZ"))


class TestCheckStatus(unittest.TestCase):
    def test_valid_status_noerror(self):
        # Valid status: NOERROR
        self.assertTrue(LogCollector._check_status("NOERROR"))

    def test_valid_status_nxdomain(self):
        # Valid status: NXDOMAIN
        self.assertTrue(LogCollector._check_status("NXDOMAIN"))

    def test_invalid_status_lowercase(self):
        # Invalid status: Correct status but lowercase
        self.assertFalse(LogCollector._check_status("noerror"))

    def test_invalid_status(self):
        # Invalid status: Unknown status
        self.assertFalse(LogCollector._check_status("WRONG"))


class TestCheckDomainName(unittest.TestCase):
    def test_valid_domain(self):
        # Valid domain name
        self.assertTrue(LogCollector._check_domain_name("example.com"))

    def test_valid_subdomain(self):
        # Valid subdomain
        self.assertTrue(LogCollector._check_domain_name("sub.example.com"))

    def test_invalid_domain_with_scheme(self):
        # Invalid domain with scheme
        self.assertFalse(LogCollector._check_domain_name("http://example.com"))

    def test_invalid_domain_with_invalid_chars(self):
        # Invalid domain with invalid characters
        self.assertFalse(LogCollector._check_domain_name("example$.com"))

    def test_invalid_domain_too_long(self):
        # Invalid domain name too long
        self.assertFalse(LogCollector._check_domain_name("a" * 256 + ".com"))

    def test_invalid_domain_too_short(self):
        # Invalid domain name: none
        self.assertFalse(LogCollector._check_domain_name(".com"))

    def test_invalid_domain_start_with_dash(self):
        # Invalid domain name starts with a dash
        self.assertFalse(LogCollector._check_domain_name("-example.com"))

    def test_invalid_domain_end_with_dash(self):
        # Invalid domain name ends with a dash
        self.assertFalse(LogCollector._check_domain_name("example-.com"))


class TestCheckRecordType(unittest.TestCase):
    def test_valid_record_type_a(self):
        # Valid status: NOERROR
        self.assertTrue(LogCollector._check_record_type("A"))

    def test_valid_record_type_aaaa(self):
        # Valid status: NXDOMAIN
        self.assertTrue(LogCollector._check_record_type("AAAA"))

    def test_invalid_record_type(self):
        # Invalid status: Unknown status
        self.assertFalse(LogCollector._check_record_type("WRONG"))


class TestCheckSize(unittest.TestCase):
    def test_valid_size(self):
        # Valid size
        self.assertTrue(LogCollector._check_size("50b"))

    def test_valid_size_multiple_digits(self):
        # Valid size with multiple digits
        self.assertTrue(LogCollector._check_size("100b"))

    def test_valid_size_large_number(self):
        # Valid size - Large number
        self.assertTrue(LogCollector._check_size("10000b"))

    def test_invalid_size_wrong_unit(self):
        # Invalid size - Unit not allowed
        self.assertFalse(LogCollector._check_size("100MB"))

    def test_invalid_size_missing_unit(self):
        # Invalid size - Unit missing
        self.assertFalse(LogCollector._check_size("100"))

    def test_invalid_size_non_numeric(self):
        # Invalid size - Non-numeric value
        self.assertFalse(LogCollector._check_size("abc"))


if __name__ == '__main__':
    unittest.main()
