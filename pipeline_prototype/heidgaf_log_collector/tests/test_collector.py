import unittest

from pipeline_prototype.heidgaf_log_collector.collector import LogCollector


class TestCheckLength(unittest.TestCase):
    def test_valid_length(self):
        # Valid length of 8
        parts = ["1", "2", "3", "4", "5", "6", "7", "8"]
        self.assertTrue(LogCollector.check_length(parts))

    def test_invalid_length_too_small(self):
        # Invalid length: Length less than 8
        parts = ["1", "2", "3", "4", "5", "6", "7"]
        self.assertFalse(LogCollector.check_length(parts))

    def test_invalid_length_too_large(self):
        # Invalid length: Length more than 8
        parts = ["1", "2", "3", "4", "5", "6", "7", "8", "9"]
        self.assertFalse(LogCollector.check_length(parts))

    def test_invalid_length_zero(self):
        # Invalid length: Length is 0
        parts = []
        self.assertFalse(LogCollector.check_length(parts))


class TestCheckTimestamp(unittest.TestCase):
    def test_valid_timestamp(self):
        # Valid timestamp
        self.assertTrue(LogCollector.check_timestamp("2024-05-21T19:29:05.466Z"))

    def test_invalid_timestamp_wrong_format(self):
        # Invalid timestamp - Wrong format
        self.assertFalse(LogCollector.check_timestamp("2023/05/21 15:30:45.123Z"))

    def test_invalid_timestamp_missing_milliseconds(self):
        # Invalid timestamp - Missing milliseconds
        self.assertFalse(LogCollector.check_timestamp("2023-05-21T15:30:45Z"))

    def test_invalid_timestamp_missing_Z(self):
        # Invalid timestamp - Missing 'Z'
        self.assertFalse(LogCollector.check_timestamp("2023-05-21T15:30:45.123"))

    def test_invalid_timestamp_incomplete(self):
        # Invalid timestamp - Incomplete timestamp
        self.assertFalse(LogCollector.check_timestamp("2023-05-21T15:30"))

    def test_invalid_timestamp_non_numeric(self):
        # Invalid timestamp - Non-numeric value
        self.assertFalse(LogCollector.check_timestamp("abcd-ef-ghTij:kl:mn.opqZ"))


class TestCheckStatus(unittest.TestCase):
    def test_valid_status_noerror(self):
        # Valid status: NOERROR
        self.assertTrue(LogCollector.check_status("NOERROR"))

    def test_valid_status_nxdomain(self):
        # Valid status: NXDOMAIN
        self.assertTrue(LogCollector.check_status("NXDOMAIN"))

    def test_invalid_status_lowercase(self):
        # Invalid status: Correct status but lowercase
        self.assertFalse(LogCollector.check_status("noerror"))

    def test_invalid_status(self):
        # Invalid status: Unknown status
        self.assertFalse(LogCollector.check_status("WRONG"))


class TestCheckDomainName(unittest.TestCase):
    def test_valid_domain(self):
        # Valid domain name
        self.assertTrue(LogCollector.check_domain_name("example.com"))

    def test_valid_subdomain(self):
        # Valid subdomain
        self.assertTrue(LogCollector.check_domain_name("sub.example.com"))

    def test_invalid_domain_with_scheme(self):
        # Invalid domain with scheme
        self.assertFalse(LogCollector.check_domain_name("http://example.com"))

    def test_invalid_domain_with_invalid_chars(self):
        # Invalid domain with invalid characters
        self.assertFalse(LogCollector.check_domain_name("example$.com"))

    def test_invalid_domain_too_long(self):
        # Invalid domain name too long
        self.assertFalse(LogCollector.check_domain_name("a" * 256 + ".com"))

    def test_invalid_domain_too_short(self):
        # Invalid domain name: none
        self.assertFalse(LogCollector.check_domain_name(".com"))

    def test_invalid_domain_start_with_dash(self):
        # Invalid domain name starts with a dash
        self.assertFalse(LogCollector.check_domain_name("-example.com"))

    def test_invalid_domain_end_with_dash(self):
        # Invalid domain name ends with a dash
        self.assertFalse(LogCollector.check_domain_name("example-.com"))


class TestCheckRecordType(unittest.TestCase):
    def test_valid_record_type_a(self):
        # Valid status: NOERROR
        self.assertTrue(LogCollector.check_record_type("A"))

    def test_valid_record_type_aaaa(self):
        # Valid status: NXDOMAIN
        self.assertTrue(LogCollector.check_record_type("AAAA"))

    def test_invalid_record_type(self):
        # Invalid status: Unknown status
        self.assertFalse(LogCollector.check_record_type("WRONG"))


class TestCheckSize(unittest.TestCase):
    def test_valid_size(self):
        # Valid size
        self.assertTrue(LogCollector.check_size("50b"))

    def test_valid_size_multiple_digits(self):
        # Valid size with multiple digits
        self.assertTrue(LogCollector.check_size("100b"))

    def test_valid_size_large_number(self):
        # Valid size - Large number
        self.assertTrue(LogCollector.check_size("10000b"))

    def test_invalid_size_wrong_unit(self):
        # Invalid size - Unit not allowed
        self.assertFalse(LogCollector.check_size("100MB"))

    def test_invalid_size_missing_unit(self):
        # Invalid size - Unit missing
        self.assertFalse(LogCollector.check_size("100"))

    def test_invalid_size_non_numeric(self):
        # Invalid size - Non-numeric value
        self.assertFalse(LogCollector.check_size("abc"))


if __name__ == '__main__':
    unittest.main()
