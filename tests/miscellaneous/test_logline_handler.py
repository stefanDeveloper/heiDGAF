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

MOCK_REQUIRED_FIELDS = ["ts", "status_code"]


class TestInit(unittest.TestCase):
    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)# Empty file to make the directory a Python package
    @patch("src.base.logline_handler.LoglineHandler._create_instance_from_list_entry")
    def test_init_successful(self, mock_create):
        validation_config=[
            ["ts", "Timestamp", "%Y-%m-%dT%H:%M:%S.%fZ"],
            ["record_type", "RegEx", r"^\d+b$"],
            ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
            ["src_ip", "IpAddress"],
        ]
        timestamp_instance = MagicMock()
        timestamp_instance.name = "ts"
        regex_instance = MagicMock()
        regex_instance.name = "record_type"
        list_item_instance = MagicMock()
        list_item_instance.name = "status_code"
        ip_address_instance = MagicMock()
        ip_address_instance.name = "src_ip"
        
        mock_relevance_handler = MagicMock()

        mock_create.side_effect = [
            timestamp_instance,
            regex_instance,
            list_item_instance,
            ip_address_instance,
        ]

        expected_log_configuration_instances = {
                "ts": timestamp_instance,
                "record_type": regex_instance,
                "status_code": list_item_instance,
                "src_ip": ip_address_instance,
        }

        with patch("src.base.logline_handler.RelevanceHandler") as mock_relevance:
            # Make the patched class return a dummy object instead of running real logic
            mock_relevance.return_value = MagicMock()

            # Run the code under test
            sut = LoglineHandler(validation_config=validation_config)        
            sut.relvance_handler = mock_relevance_handler

            # Assert
            mock_relevance.assert_called_once()
            mock_relevance.assert_called_once_with(log_configuration_instances=expected_log_configuration_instances)

    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    @patch("src.base.logline_handler.LoglineHandler._create_instance_from_list_entry")
    def test_init_multiple_fields_with_same_name(self, mock_create):
        validation_config=[
            ["ts", "Timestamp", "%Y-%m-%dT%H:%M:%S.%fZ"],
            ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
            ["status_code", "RegEx", r"^\d+b$"],
            ["src_ip", "IpAddress"],
        ]
        regex_1_instance = MagicMock()
        regex_1_instance.name = "timestamp"
        list_item_instance = MagicMock()
        list_item_instance.name = "status_code"
        regex_2_instance = MagicMock()
        regex_2_instance.name = "status_code"
        ip_address_instance = MagicMock()
        ip_address_instance.name = "src_ip"

        mock_create.side_effect = [
            regex_1_instance,
            list_item_instance,
            regex_2_instance,
            ip_address_instance,
        ]

        # Act and Assert
        with self.assertRaises(ValueError) as context:
            LoglineHandler(validation_config=validation_config)

        self.assertEqual(str(context.exception), "Multiple fields with same name")

    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    @patch("src.base.logline_handler.LoglineHandler._create_instance_from_list_entry")
    def test_init_missing_fields(self, mock_create):
        validation_config=[
            ["ts", "RegEx", r"^\d+b$"],
            ["src_ip", "IpAddress"],
        ]
        regex_1_instance = MagicMock()
        regex_1_instance.name = "timestamp"
        ip_address_instance = MagicMock()
        ip_address_instance.name = "src_ip"

        mock_create.side_effect = [regex_1_instance, ip_address_instance]

        # Act and Assert
        with self.assertRaises(ValueError) as context:
            LoglineHandler(validation_config=validation_config)

        self.assertEqual(
            str(context.exception), "Not all needed fields are set in the configuration"
        )

    @patch("src.base.logline_handler.REQUIRED_FIELDS", ["field_1"])
    @patch("src.base.logline_handler.FORBIDDEN_FIELD_NAMES", ["field_2"])
    @patch("src.base.logline_handler.LoglineHandler._create_instance_from_list_entry")
    def test_init_no_fields(self, mock_create):
        # Arrange
        ip_address_instance = MagicMock()
        ip_address_instance.name = "field_2"
        mock_create.side_effect = [ip_address_instance]

        # Act and Assert
        with self.assertRaises(ValueError) as context:
            LoglineHandler(validation_config=["field_1", "field_2"])

        self.assertEqual(
            str(context.exception),
            "Forbidden field name included. "
            "These fields are used internally and cannot be used as names: "
            "['field_2']",
        )

    @patch("src.base.logline_handler.REQUIRED_FIELDS", [])
    @patch("src.base.logline_handler.LoglineHandler._create_instance_from_list_entry")
    def test_init_forbidden_fields(self, mock_create):
        # Arrange
        mock_create.side_effect = []

        # Act and Assert
        with self.assertRaises(ValueError) as context:
            LoglineHandler(validation_config=[])
            self.assertEqual(str(context.exception), "No fields configured")


class TestValidateLogline(unittest.TestCase):
    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    def test_validate_successful(self):
        validation_config=[
            ["ts", "Timestamp", "%Y-%m-%dT%H:%M:%S.%fZ"],
            ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
            ["src_ip", "IpAddress"],
        ]
        sut = LoglineHandler(validation_config=validation_config)

        # Act and Assert
        self.assertTrue(
            sut.validate_logline('{"ts": "2024-07-28T14:45:30.123Z","status_code": "NXDOMAIN","src_ip": "126.24.5.20"}')
        )

    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    def test_validate_successful_with_real_format(self):
        validation_config = [
            ["ts", "Timestamp", "%Y-%m-%dT%H:%M:%S.%f"],
            ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
            ["src_ip", "IpAddress"],
            ["dns_server_ip", "IpAddress"],
            [
                "domain_name",
                "RegEx",
                r"^(?=.{1,253}$)((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,63}$",
            ],
            ["record_type", "ListItem", ["A", "AAAA"]],
            ["response_ip", "IpAddress"],
            ["size", "RegEx", r'^\d+$'],
        ]
        sut = LoglineHandler(validation_config=validation_config)

        # Act and Assert
        self.assertTrue(
            sut.validate_logline(
                '{"ts": "2024-07-28T14:45:30.123","status_code": "NXDOMAIN","dns_server_ip": "127.0.0.2", "domain_name": "domain.test", "record_type": "A", "src_ip": "126.24.5.20", "response_ip": "fe80::1", "size": "150"}'
            )
        )

    @patch("src.base.logline_handler.logger")
    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    def test_validate_wrong_number_of_fields(self, mock_logger):
        validation_config =[
            ["ts", "RegEx", r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"],
            ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
            ["src_ip", "IpAddress"],
        ]
        sut = LoglineHandler(validation_config=validation_config)

        # Act and Assert
        self.assertFalse(
            sut.validate_logline('{"ts": "2024-07-28T14:45:30.123","status_code": "NXDOMAIN", "src_ip": "126.24.5.20"}')
                

        )

    @patch("src.base.logline_handler.logger")
    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    def test_validate_empty(self, mock_logger):
        validation_config= [
            ["ts", "RegEx", r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"],
            ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
            ["src_ip", "IpAddress"],
        ]
        sut = LoglineHandler(validation_config=validation_config)

        # Act and Assert
        self.assertFalse(sut.validate_logline("{}"))

    @patch("src.base.logline_handler.logger")
    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    def test_validate_contains_invalid_fields(self, mock_logger):
        validation_config = [
            ["ts", "RegEx", r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"],
            ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
            ["src_ip", "IpAddress"],
        ]
        sut = LoglineHandler(validation_config=validation_config)

        # Act and Assert
        self.assertFalse(
            sut.validate_logline('{"ts": "2024-07-28T14:45:30.123","status_code": "NXDOMAIN", "src_ip": "126.24.5.300"}')
        )

    @patch("src.base.logline_handler.logger")
    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    def test_validate_wrong_order_of_fields(self, mock_logger):
        validation_config = [
            ["ts", "RegEx", r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"],
            ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
            ["src_ip", "IpAddress"],
        ]
        sut = LoglineHandler(validation_config=validation_config)
        # Act and Assert
        self.assertFalse(
            sut.validate_logline('{"ts": "2024-07-28T14:45:30.123", "src_ip": "126.24.5.20"}')
        )


class TestValidateLoglineAndGetFieldsAsJson(unittest.TestCase):
    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    def test_validate_true(self):
        validation_config = [
                ["ts", "Timestamp", "%Y-%m-%dT%H:%M:%S.%fZ"],
                ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
                ["src_ip", "IpAddress"],
                ["dns_server_ip", "IpAddress"],
                [ "domain_name", "RegEx", '^(?=.{1,253}$)((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,63}$' ],
                ["record_type", "ListItem", ["A", "AAAA"]],
                ["response_ip", "IpAddress"],
                ["size", "RegEx", r"^\d+$"],
            ]
        # Arrange
        expected_result = {
            "ts": "2024-07-28T14:45:30.123Z",
            "status_code": "NXDOMAIN",
            "src_ip": "127.0.0.2",
            "dns_server_ip": "126.24.5.20",
            "domain_name": "domain.test",
            "record_type": "A",
            "response_ip": "fe80::1",
            "size": "150",
        }

        sut = LoglineHandler(validation_config=validation_config)
        # Act and Assert
        self.assertEqual(
            expected_result,
            sut.validate_logline_and_get_fields_as_json(
                '{"ts": "2024-07-28T14:45:30.123Z", "status_code": "NXDOMAIN", "src_ip": "127.0.0.2", "domain_name": "domain.test", "record_type": "A", "dns_server_ip": "126.24.5.20", "response_ip": "fe80::1", "size": "150"}'
            )
        )

    @patch("src.base.logline_handler.logger")
    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    def test_validate_false(self, mock_logger):
        validation_config = [
                ["ts", "RegEx", r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"],
                ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
                ["src_ip", "IpAddress"],
            ]
        sut = LoglineHandler(validation_config=validation_config)

        # Act and Assert
        with self.assertRaises(ValueError) as context:
            sut.validate_logline_and_get_fields_as_json(
            '{"ts": "2024-07-28T14:45:30.123","status_code": "NXDOMAIN", "src_ip": "126.24.5.20"}'
            )

            self.assertEqual(
                str(context.exception), "Incorrect logline, validation unsuccessful"
            )


class TestCheckRelevance(unittest.TestCase):
    @patch("src.base.logline_handler.REQUIRED_FIELDS", MOCK_REQUIRED_FIELDS)
    def setUp(self):
        # Arrange
        validation_config = [
            ["ts", "RegEx", r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"],
            ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
            ["test_1", "ListItem", ["123", "456"]],
            ["src_ip", "IpAddress"],
            ["test_2", "ListItem", ["789", "TEST"], ["TEST"]],
        ]
        self.sut = LoglineHandler(validation_config=validation_config)

    def test_check_relevant_value_no_relevance_check(self):
        # Act and Assert
        self.assertTrue(
            self.sut.check_relevance(
                {
                    "ts": "2024-07-28T14:45:30.123Z",
                    "status_code": "NXDOMAIN",
                    "test_1": "123",
                    "src_ip": "10.0.0.4",
                    "test_2": "TEST",
                },
                "no_relevance_check"
            )
        )
    def test_check_relevant_value_check_dga_relevance(self):
        # Act and Assert
        self.assertTrue(
            self.sut.check_relevance(
                {
                    "ts": "2024-07-28T14:45:30.123Z",
                    "status_code": "NXDOMAIN",
                    "test_1": "123",
                    "src_ip": "10.0.0.4",
                    "test_2": "TEST",
                },
                "check_dga_relevance"
            )
        )
        
    def test_check_irrelevant_value_1_check_dga_relevance(self):
        # incorrect in all lists
        # Act and Assert
        self.assertFalse(
            self.sut.check_relevance(
                {
                    "ts": "2024-07-28T14:45:30.123Z",
                    "status_code": "NOERROR",
                    "test_1": "123",
                    "src_ip": "10.0.0.4",
                    "test_2": "789",
                },
                "check_dga_relevance"
            )
        )

    def test_check_irrelevant_value_2_check_dga_relevance(self):
        # correct in one list, but not all
        # Act and Assert
        self.assertFalse(
            self.sut.check_relevance(
                {
                    "ts": "2024-07-28T14:45:30.123Z",
                    "status_code": "NXDOMAIN",
                    "test_1": "123",
                    "src_ip": "10.0.0.4",
                    "test_2": "789",
                },
                "check_dga_relevance"
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

    def test_invalid_timestamp_parameters(self):
        # Arrange
        field_list = ["test_name", "Timestamp"]

        # Act & Assert
        with self.assertRaises(ValueError) as context:
            LoglineHandler._create_instance_from_list_entry(field_list)

        # Assert
        self.assertEqual(str(context.exception), "Invalid Timestamp parameters")

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
