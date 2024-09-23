import unittest
from unittest.mock import patch, MagicMock

from src.base.logline_handler import LoglineHandler

MOCK_REQUIRED_FIELDS = ["timestamp", "status_code"]


class TestInit(unittest.TestCase):
    @patch('src.base.logline_handler.REQUIRED_FIELDS', MOCK_REQUIRED_FIELDS)
    @patch('src.base.logline_handler.CONFIG', {
        "loglines": {
            "fields": [
                ["timestamp", "RegEx", r'^\d+b$'],
                ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
                ["client_ip", "IpAddress"],
            ]
        }
    })
    @patch('src.base.logline_handler.LoglineHandler._create_instance_from_list_entry')
    def test_init_successful(self, mock_create):
        # Arrange
        regex_instance = MagicMock()
        regex_instance.name = "timestamp"
        list_item_instance = MagicMock()
        list_item_instance.name = "status_code"
        ip_address_instance = MagicMock()
        ip_address_instance.name = "client_ip"

        mock_create.side_effect = [regex_instance, list_item_instance, ip_address_instance]

        expected_instances_by_name = {
            "timestamp": regex_instance,
            "status_code": list_item_instance,
            "client_ip": ip_address_instance,
        }
        expected_instances_by_position = {
            0: regex_instance,
            1: list_item_instance,
            2: ip_address_instance,
        }

        # Act
        sut = LoglineHandler()

        # Assert
        self.assertEqual(expected_instances_by_name, sut.instances_by_name)
        self.assertEqual(expected_instances_by_position, sut.instances_by_position)
        self.assertEqual(3, sut.number_of_fields)

    @patch('src.base.logline_handler.REQUIRED_FIELDS', MOCK_REQUIRED_FIELDS)
    @patch('src.base.logline_handler.CONFIG', {
        "loglines": {
            "fields": [
                ["timestamp", "RegEx", r'^\d+b$'],
                ["status_code", "ListItem", ["NOERROR", "NXDOMAIN"], ["NXDOMAIN"]],
                ["status_code", "RegEx", r'^\d+b$'],
                ["client_ip", "IpAddress"],
            ]
        }
    })
    @patch('src.base.logline_handler.LoglineHandler._create_instance_from_list_entry')
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

        mock_create.side_effect = [regex_1_instance, list_item_instance, regex_2_instance, ip_address_instance]

        # Act and Assert
        with self.assertRaises(ValueError) as context:
            LoglineHandler()

        self.assertEqual(str(context.exception), "Multiple fields with same name")

    @patch('src.base.logline_handler.REQUIRED_FIELDS', MOCK_REQUIRED_FIELDS)
    @patch('src.base.logline_handler.CONFIG', {
        "loglines": {
            "fields": [
                ["timestamp", "RegEx", r'^\d+b$'],
                ["client_ip", "IpAddress"],
            ]
        }
    })
    @patch('src.base.logline_handler.LoglineHandler._create_instance_from_list_entry')
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

        self.assertEqual(str(context.exception), "Not all needed fields are set in the configuration")

    @patch('src.base.logline_handler.REQUIRED_FIELDS', [])
    @patch('src.base.logline_handler.CONFIG', {
        "loglines": {
            "fields": []
        }
    })
    @patch('src.base.logline_handler.LoglineHandler._create_instance_from_list_entry')
    def test_init_no_fields(self, mock_create):
        # Arrange
        mock_create.side_effect = []

        # Act and Assert
        with self.assertRaises(ValueError) as context:
            LoglineHandler()

        self.assertEqual(str(context.exception), "No fields configured")


if __name__ == '__main__':
    unittest.main()

# sut = LoglineHandler()
# print(sut.validate_logline_and_get_fields_as_json(
#     "2024-07-28T14:45:30.123Z NXDOMAIN 127.0.0.2 126.24.5.304 domain.test A fe80::1 150"
# ))
