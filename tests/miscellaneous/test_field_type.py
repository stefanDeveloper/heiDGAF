import ipaddress
import re
import unittest

from src.base.logline_handler import FieldType, RegEx, IpAddress, ListItem, Timestamp


class TestFieldType(unittest.TestCase):
    def test_init(self):
        # Arrange
        name = "test_name"

        # Act
        sut = FieldType(name=name)

        # Assert
        self.assertEqual(name, sut.name)

    def test_validate(self):
        sut = FieldType(name="test_name")

        with self.assertRaises(NotImplementedError):
            sut.validate(value="test")


class TestTimestamp(unittest.TestCase):
    def test_init(self):
        # Arrange
        name = "test_name"
        timestamp_format = "%Y-%m-%dT%H:%M:%S.%fZ"

        # Act
        sut = Timestamp(name=name, timestamp_format=timestamp_format)

        # Assert
        self.assertEqual(name, sut.name)
        self.assertEqual(timestamp_format, sut.timestamp_format)

    def test_validate_successful(self):
        # Arrange
        name = "test_name"
        timestamp_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        sut = Timestamp(name=name, timestamp_format=timestamp_format)

        # Act and Assert
        self.assertTrue(sut.validate(value="2024-07-28T14:45:30.123Z"))

    def test_validate_unsuccessful(self):
        # Arrange
        name = "test_name"
        timestamp_format = "%Y-%m-%d %H:%M:%S.%f"
        sut = Timestamp(name=name, timestamp_format=timestamp_format)

        # Act and Assert
        self.assertFalse(sut.validate(value="2024-07-28T14:45:30.123Z"))


class TestRegEx(unittest.TestCase):
    def test_init(self):
        # Arrange
        name = "test_name"
        pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"

        # Act
        sut = RegEx(name=name, pattern=pattern)

        # Assert
        self.assertEqual(name, sut.name)
        self.assertEqual(
            re.compile("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$"),
            sut.pattern,
        )

    def test_validate_successful(self):
        # Arrange
        name = "test_name"
        pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"
        sut = RegEx(name=name, pattern=pattern)

        # Act and Assert
        self.assertTrue(sut.validate(value="2024-07-28T14:45:30.123Z"))

    def test_validate_unsuccessful(self):
        # Arrange
        name = "test_name"
        pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"
        sut = RegEx(name=name, pattern=pattern)

        # Act and Assert
        self.assertFalse(sut.validate(value="2024-07-28T14:45:30.123"))


class TestIpAddress(unittest.TestCase):
    def test_init(self):
        # Arrange
        name = "test_name"

        # Act
        sut = IpAddress(name=name)

        # Assert
        self.assertEqual(name, sut.name)

    def test_validate_successful_ipv4(self):
        # Arrange
        name = "test_name"
        sut = IpAddress(name=name)

        # Act and Assert
        self.assertTrue(sut.validate("192.0.17.4"))

    def test_validate_successful_ipv6(self):
        # Arrange
        name = "test_name"
        sut = IpAddress(name=name)

        # Act and Assert
        self.assertTrue(sut.validate(ipaddress.IPv6Address("fe80::1")))

    def test_validate_unsuccessful(self):
        # Arrange
        name = "test_name"
        sut = IpAddress(name=name)

        # Act and Assert
        self.assertFalse(sut.validate("117.45.2.319"))


class TestListItem(unittest.TestCase):
    def test_init_successful(self):
        # Arrange
        name = "test_name"
        allowed_list = ["test", 23, "another_test"]
        relevant_list = [23]

        # Act
        sut = ListItem(
            name=name, allowed_list=allowed_list, relevant_list=relevant_list
        )

        # Assert
        self.assertEqual(name, sut.name)
        self.assertEqual(allowed_list, sut.allowed_list)
        self.assertEqual(relevant_list, sut.relevant_list)

    def test_init_unsuccessful(self):
        # Arrange
        name = "test_name"
        allowed_list = ["test", 23, "another_test"]
        relevant_list = [48]

        # Act and Assert
        with self.assertRaises(ValueError):
            sut = ListItem(
                name=name, allowed_list=allowed_list, relevant_list=relevant_list
            )

    def test_validate_successful(self):
        # Arrange
        name = "test_name"
        allowed_list = ["test", 23, "another_test"]
        relevant_list = [23]
        sut = ListItem(
            name=name, allowed_list=allowed_list, relevant_list=relevant_list
        )

        # Act and Assert
        self.assertTrue(sut.validate("test"))
        self.assertTrue(sut.validate(23))
        self.assertTrue(sut.validate("another_test"))

    def test_validate_unsuccessful(self):
        # Arrange
        name = "test_name"
        allowed_list = ["test", 23, "another_test"]
        relevant_list = [23]
        sut = ListItem(
            name=name, allowed_list=allowed_list, relevant_list=relevant_list
        )

        # Act and Assert
        self.assertFalse(sut.validate("Test"))
        self.assertFalse(sut.validate(48))

    # def test_check_relevance_true(self):
    #     # Arrange
    #     name = "test_name"
    #     allowed_list = ["test", 23, "another_test", 48]
    #     relevant_list = [23, 48]
    #     sut = ListItem(
    #         name=name, allowed_list=allowed_list, relevant_list=relevant_list
    #     )

    #     # Act and Assert
    #     self.assertTrue(sut.check_relevance(23))

    # def test_check_relevance_false(self):
    #     # Arrange
    #     name = "test_name"
    #     allowed_list = ["test", 23, "another_test", 48]
    #     relevant_list = [23, 48]
    #     sut = ListItem(
    #         name=name, allowed_list=allowed_list, relevant_list=relevant_list
    #     )

    #     # Act and Assert
    #     self.assertFalse(sut.check_relevance("this_is_not_relevant"))

    # def test_check_relevance_no_relevant_values(self):
    #     # Arrange
    #     name = "test_name"
    #     allowed_list = ["test", 23, "another_test", 48]
    #     relevant_list = []
    #     sut = ListItem(
    #         name=name, allowed_list=allowed_list, relevant_list=relevant_list
    #     )

    #     # Act and Assert
    #     self.assertTrue(sut.check_relevance("this_is_not_relevant"))


if __name__ == "__main__":
    unittest.main()
