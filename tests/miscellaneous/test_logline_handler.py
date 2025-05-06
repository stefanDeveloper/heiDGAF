import datetime
import re
import unittest
from unittest.mock import patch, MagicMock

from src.base.logline_handler import (
    LoglineHandler,
    RegEx,
    ListItem,
    IpAddress,
    Timestamp,
)

MOCK_REQUIRED_FIELDS = ["timestamp", "status_code"]


class TestInit(unittest.TestCase):
    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    @patch(
        "src.base.logline_handler.LOGLINE_FIELDS",
        [
            ["timestamp", "Timestamp", "%Y-%m-%dT%H:%M:%S.%fZ"],
            ["record_type", "RegEx", r"^\d+b$"],
            ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
            ["client_ip", "IpAddress"],
        ],
    )
    @patch("src.base.logline_handler.LoglineHandler._create_instance_from_list_entry")
    def test_init_successful(self, mock_create):
        # Arrange
        timestamp_instance = MagicMock()
        timestamp_instance.name = "timestamp"
        regex_instance = MagicMock()
        regex_instance.name = "record_type"
        list_item_instance = MagicMock()
        list_item_instance.name = "status_code"
        ip_address_instance = MagicMock()
        ip_address_instance.name = "client_ip"

        mock_create.side_effect = [
            timestamp_instance,
            regex_instance,
            list_item_instance,
            ip_address_instance,
        ]

        expected_instances_by_name = {
            "timestamp": timestamp_instance,
            "record_type": regex_instance,
            "status_code": list_item_instance,
            "client_ip": ip_address_instance,
        }
        expected_instances_by_position = {
            0: timestamp_instance,
            1: regex_instance,
            2: list_item_instance,
            3: ip_address_instance,
        }

        # Act
        sut = LoglineHandler()

        # Assert
        self.assertEqual(expected_instances_by_name, sut.instances_by_name)
        self.assertEqual(expected_instances_by_position, sut.instances_by_position)
        self.assertEqual(4, sut.number_of_fields)

    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    @patch(
        "src.base.logline_handler.LOGLINE_FIELDS",
        [
            ["timestamp", "Timestamp", "%Y-%m-%dT%H:%M:%S.%fZ"],
            ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
            ["status_code", "RegEx", r"^\d+b$"],
            ["client_ip", "IpAddress"],
        ],
    )
    @patch("src.base.logline_handler.LoglineHandler._create_instance_from_list_entry")
    def test_init_multiple_fields_with_same_name(self, mock_create):
        # Arrange
        regex_1_instance = MagicMock()
        regex_1_instance.name = "timestamp"
        list_item_instance = MagicMock()
        list_item_instance.name = "status_code"
        regex_2_instance = MagicMock()
        regex_2_instance.name = "status_code"
        ip_address_instance = MagicMock()
        ip_address_instance.name = "client_ip"

        mock_create.side_effect = [
            regex_1_instance,
            list_item_instance,
            regex_2_instance,
            ip_address_instance,
        ]

        # Act and Assert
        with self.assertRaises(ValueError) as context:
            LoglineHandler()

        self.assertEqual(str(context.exception), "Multiple fields with same name")

    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    @patch(
        "src.base.logline_handler.LOGLINE_FIELDS",
        [
            ["timestamp", "RegEx", r"^\d+b$"],
            ["client_ip", "IpAddress"],
        ],
    )
    @patch("src.base.logline_handler.LoglineHandler._create_instance_from_list_entry")
    def test_init_missing_fields(self, mock_create):
        # Arrange
        regex_1_instance = MagicMock()
        regex_1_instance.name = "timestamp"
        ip_address_instance = MagicMock()
        ip_address_instance.name = "client_ip"

        mock_create.side_effect = [regex_1_instance, ip_address_instance]

        # Act and Assert
        with self.assertRaises(ValueError) as context:
            LoglineHandler()

        self.assertEqual(
            str(context.exception), "Not all needed fields are set in the configuration"
        )

    @patch("src.base.logline_handler.REQUIRED_FIELDS", ["field_1"])
    @patch("src.base.logline_handler.LOGLINE_FIELDS", ["field_1", "field_2"])
    @patch("src.base.logline_handler.FORBIDDEN_FIELD_NAMES", ["field_2"])
    @patch("src.base.logline_handler.LoglineHandler._create_instance_from_list_entry")
    def test_init_no_fields(self, mock_create):
        # Arrange
        ip_address_instance = MagicMock()
        ip_address_instance.name = "field_2"
        mock_create.side_effect = [ip_address_instance]

        # Act and Assert
        with self.assertRaises(ValueError) as context:
            LoglineHandler()

        self.assertEqual(
            str(context.exception),
            "Forbidden field name included. "
            "These fields are used internally and cannot be used as names: "
            "['field_2']",
        )

    @patch("src.base.logline_handler.REQUIRED_FIELDS", [])
    @patch("src.base.logline_handler.LOGLINE_FIELDS", [])
    @patch("src.base.logline_handler.LOGLINE_FIELDS", [])
    @patch("src.base.logline_handler.LoglineHandler._create_instance_from_list_entry")
    def test_init_forbidden_fields(self, mock_create):
        # Arrange
        mock_create.side_effect = []

        # Act and Assert
        with self.assertRaises(ValueError) as context:
            LoglineHandler()

        self.assertEqual(str(context.exception), "No fields configured")


class TestValidateLogline(unittest.TestCase):
    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    @patch(
        "src.base.logline_handler.LOGLINE_FIELDS",
        [
            ["timestamp", "Timestamp", "%Y-%m-%dT%H:%M:%S.%fZ"],
            ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
            ["client_ip", "IpAddress"],
        ],
    )
    def test_validate_successful(self):
        # Arrange
        sut = LoglineHandler()

        # Act and Assert
        self.assertTrue(
            sut.validate_logline("2024-07-28T14:45:30.123Z NXDOMAIN 126.24.5.20")
        )

    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    @patch(
        "src.base.logline_handler.LOGLINE_FIELDS",
        [
            ["timestamp", "Timestamp", "%Y-%m-%dT%H:%M:%S.%f"],
            ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
            ["client_ip", "IpAddress"],
            ["dns_server_ip", "IpAddress"],
            [
                "domain_name",
                "RegEx",
                r"^(?=.{1,253}$)((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,63}$",
            ],
            ["record_type", "ListItem", ["A", "AAAA"]],
            ["response_ip", "IpAddress"],
            ["size", "RegEx", r"^\d+b$"],
        ],
    )
    def test_validate_successful_with_real_format(self):
        # Arrange
        sut = LoglineHandler()

        # Act and Assert
        self.assertTrue(
            sut.validate_logline(
                "2024-07-28T14:45:30.123 NXDOMAIN 127.0.0.2 126.24.5.20 domain.test A fe80::1 150b"
            )
        )

    @patch("src.base.logline_handler.logger")
    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    @patch(
        "src.base.logline_handler.LOGLINE_FIELDS",
        [
            ["timestamp", "RegEx", r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"],
            ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
            ["client_ip", "IpAddress"],
        ],
    )
    def test_validate_wrong_number_of_fields(self, mock_logger):
        # Arrange
        sut = LoglineHandler()

        # Act and Assert
        self.assertFalse(
            sut.validate_logline("2024-07-28T14:45:30.123Z NXDOMAIN 126.24.5.20 test")
        )

    @patch("src.base.logline_handler.logger")
    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    @patch(
        "src.base.logline_handler.LOGLINE_FIELDS",
        [
            ["timestamp", "RegEx", r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"],
            ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
            ["client_ip", "IpAddress"],
        ],
    )
    def test_validate_empty(self, mock_logger):
        # Arrange
        sut = LoglineHandler()

        # Act and Assert
        self.assertFalse(sut.validate_logline(""))

    @patch("src.base.logline_handler.logger")
    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    @patch(
        "src.base.logline_handler.LOGLINE_FIELDS",
        [
            ["timestamp", "RegEx", r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"],
            ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
            ["client_ip", "IpAddress"],
        ],
    )
    def test_validate_contains_invalid_fields(self, mock_logger):
        # Arrange
        sut = LoglineHandler()

        # Act and Assert
        self.assertFalse(
            sut.validate_logline("2024-07-28T14:45:30.123Z NXDOMAIN 126.24.5.300")
        )

    @patch("src.base.logline_handler.logger")
    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    @patch(
        "src.base.logline_handler.LOGLINE_FIELDS",
        [
            ["timestamp", "RegEx", r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"],
            ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
            ["client_ip", "IpAddress"],
        ],
    )
    def test_validate_wrong_order_of_fields(self, mock_logger):
        # Arrange
        sut = LoglineHandler()

        # Act and Assert
        self.assertFalse(
            sut.validate_logline("NXDOMAIN 2024-07-28T14:45:30.123Z 126.24.5.20")
        )


class TestValidateLoglineAndGetFieldsAsJson(unittest.TestCase):
    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    @patch(
        "src.base.logline_handler.LOGLINE_FIELDS",
        [
            ["timestamp", "Timestamp", "%Y-%m-%dT%H:%M:%S.%fZ"],
            ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
            ["client_ip", "IpAddress"],
            ["dns_server_ip", "IpAddress"],
            [
                "domain_name",
                "RegEx",
                r"^(?=.{1,253}$)((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,63}$",
            ],
            ["record_type", "ListItem", ["A", "AAAA"]],
            ["response_ip", "IpAddress"],
            ["size", "RegEx", r"^\d+b$"],
        ],
    )
    def test_validate_true(self):
        # Arrange
        expected_result = {
            "timestamp": "2024-07-28T14:45:30.123000",
            "status_code": "NXDOMAIN",
            "client_ip": "127.0.0.2",
            "dns_server_ip": "126.24.5.20",
            "domain_name": "domain.test",
            "record_type": "A",
            "response_ip": "fe80::1",
            "size": "150b",
        }

        sut = LoglineHandler()

        # Act and Assert
        self.assertEqual(
            expected_result,
            sut.validate_logline_and_get_fields_as_json(
                "2024-07-28T14:45:30.123Z NXDOMAIN 127.0.0.2 126.24.5.20 domain.test A fe80::1 150b"
            ),
        )

    @patch("src.base.logline_handler.logger")
    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    @patch(
        "src.base.logline_handler.LOGLINE_FIELDS",
        [
            ["timestamp", "RegEx", r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"],
            ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
            ["client_ip", "IpAddress"],
        ],
    )
    def test_validate_false(self, mock_logger):
        # Arrange
        sut = LoglineHandler()

        # Act and Assert
        with self.assertRaises(ValueError) as context:
            sut.validate_logline_and_get_fields_as_json(
                "NXDOMAIN 2024-07-28T14:45:30.123Z 126.24.5.20"
            )

        self.assertEqual(
            str(context.exception), "Incorrect logline, validation unsuccessful"
        )


class TestCheckRelevance(unittest.TestCase):
    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    @patch(
        "src.base.logline_handler.LOGLINE_FIELDS",
        [
            ["timestamp", "RegEx", r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"],
            ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
            ["test_1", "ListItem", ["123", "456"]],
            ["client_ip", "IpAddress"],
            ["test_2", "ListItem", ["789", "TEST"], ["TEST"]],
        ],
    )
    def setUp(self):
        # Arrange
        self.sut = LoglineHandler()

    def test_check_relevant_value(self):
        # Act and Assert
        self.assertTrue(
            self.sut.check_relevance(
                {
                    "timestamp": "2024-07-28T14:45:30.123Z",
                    "status_code": "NXDOMAIN",
                    "test_1": "123",
                    "client_ip": "10.0.0.4",
                    "test_2": "TEST",
                }
            )
        )

    def test_check_irrelevant_value_1(self):
        # incorrect in all lists
        # Act and Assert
        self.assertFalse(
            self.sut.check_relevance(
                {
                    "timestamp": "2024-07-28T14:45:30.123Z",
                    "status_code": "NOERROR",
                    "test_1": "123",
                    "client_ip": "10.0.0.4",
                    "test_2": "789",
                }
            )
        )

    def test_check_irrelevant_value_2(self):
        # correct in one list, but not all
        # Act and Assert
        self.assertFalse(
            self.sut.check_relevance(
                {
                    "timestamp": "2024-07-28T14:45:30.123Z",
                    "status_code": "NXDOMAIN",
                    "test_1": "123",
                    "client_ip": "10.0.0.4",
                    "test_2": "789",
                }
            )
        )


class TestCreateInstanceFromListEntry(unittest.TestCase):

    def test_create_timestamp_instance(self):
        # Arrange
        field_list = ["test_name", "Timestamp", "%Y-%m-%d %H:%M:%S.%fZ"]

        # Act
        instance = LoglineHandler._create_instance_from_list_entry(field_list)

        # Assert
        self.assertIsInstance(instance, Timestamp)
        self.assertEqual("test_name", instance.name)
        self.assertEqual("%Y-%m-%d %H:%M:%S.%fZ", instance.timestamp_format)

    def test_create_regex_instance(self):
        # Arrange
        field_list = ["test_name", "RegEx", r"pattern"]

        # Act
        instance = LoglineHandler._create_instance_from_list_entry(field_list)

        # Assert
        self.assertIsInstance(instance, RegEx)
        self.assertEqual("test_name", instance.name)
        self.assertEqual(re.compile("pattern"), instance.pattern)

    def test_create_list_item_instance(self):
        # Arrange
        field_list = [
            "test_name",
            "ListItem",
            ["item1", "item2", "rel1", "rel2"],
            ["rel1", "rel2"],
        ]

        # Act
        instance = LoglineHandler._create_instance_from_list_entry(field_list)

        # Assert
        self.assertIsInstance(instance, ListItem)
        self.assertEqual(instance.name, "test_name")
        self.assertEqual(instance.allowed_list, ["item1", "item2", "rel1", "rel2"])
        self.assertEqual(instance.relevant_list, ["rel1", "rel2"])

    def test_create_ip_address_instance(self):
        # Arrange
        field_list = ["test_name", "IpAddress"]

        # Act
        instance = LoglineHandler._create_instance_from_list_entry(field_list)

        # Assert
        self.assertIsInstance(instance, IpAddress)
        self.assertEqual(instance.name, "test_name")

    def test_create_ip_address_instance_with_false_number_of_fields(self):
        # Arrange
        field_list = ["test_name", "IpAddress", "this", "is", "wrong"]

        # Act and Assert
        with self.assertRaises(ValueError) as context:
            LoglineHandler._create_instance_from_list_entry(field_list)

        self.assertEqual(str(context.exception), "Invalid IpAddress parameters")

    def test_invalid_field_list_length(self):
        # Arrange
        field_list = ["test_name"]

        # Act & Assert
        with self.assertRaises(ValueError) as context:
            LoglineHandler._create_instance_from_list_entry(field_list)

        # Assert
        self.assertEqual(str(context.exception), "Invalid field list or field name")

    def test_invalid_class_name(self):
        # Arrange
        field_list = ["test_name", "NonExistentClass"]

        # Act & Assert
        with self.assertRaises(ValueError) as context:
            LoglineHandler._create_instance_from_list_entry(field_list)

        # Assert
        self.assertEqual(str(context.exception), "Class 'NonExistentClass' not found")

    def test_unsupported_class_name(self):
        # Arrange
        field_list = ["test_name", "LoglineHandler"]

        # Act & Assert
        with self.assertRaises(ValueError) as context:
            LoglineHandler._create_instance_from_list_entry(field_list)

        # Assert
        self.assertEqual(str(context.exception), "Unsupported class 'LoglineHandler'")

    def test_invalid_regex_parameters(self):
        # Arrange
        field_list = ["test_name", "RegEx", 123]

        # Act & Assert
        with self.assertRaises(ValueError) as context:
            LoglineHandler._create_instance_from_list_entry(field_list)

        # Assert
        self.assertEqual(str(context.exception), "Invalid RegEx parameters")

    def test_invalid_list_item_parameters(self):
        # Arrange
        field_list = ["test_name", "ListItem", "not_a_list"]

        # Act & Assert
        with self.assertRaises(ValueError) as context:
            LoglineHandler._create_instance_from_list_entry(field_list)

        # Assert
        self.assertEqual(str(context.exception), "Invalid ListItem parameters")


if __name__ == "__main__":
    unittest.main()
