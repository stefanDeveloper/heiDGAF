import datetime
import re

from src.base.log_config import get_logger
from src.base.utils import setup_config, validate_host

logger = get_logger()

CONFIG = setup_config()
LOGLINE_FIELDS = CONFIG["pipeline"]["log_collection"]["collector"]["logline_format"]
REQUIRED_FIELDS = [
    "timestamp",
    "status_code",
    "client_ip",
    "record_type",
    "domain_name",
]
FORBIDDEN_FIELD_NAMES = [
    "logline_id",
    "batch_id",
]  # field names that are used internally


class FieldType:
    """Base class for all field validation types in the logline format configuration

    Provides the common interface for field validation. All specific field types inherit
    from this class and implement their own validation logic in the :meth:`validate` method.
    """

    def __init__(self, name: str):
        self.name = name

    def validate(self, value) -> bool:
        """Validates the input value according to the field type's rules.

        This method must be implemented by all inheriting field type classes.
        Each implementation defines specific validation logic appropriate for the field type.

        Args:
            value: The value to be validated.

        Returns:
            True if the value is valid according to the field type's rules, False otherwise.

        Raises:
            NotImplementedError: This base method must be overridden by subclasses.
        """
        raise NotImplementedError


class RegEx(FieldType):
    """Field type for regular expression pattern validation

    Validates field values against a specified regular expression pattern.
    Useful for validating structured text fields like domain names, sizes, or custom formats.
    """

    def __init__(self, name: str, pattern: str):
        super().__init__(name)
        self.pattern = re.compile(r"{}".format(pattern))

    def validate(self, value) -> bool:
        """Validates the input value against the configured regular expression pattern.

        Args:
            value: The value to be validated against the regex pattern.

        Returns:
            True if the value matches the pattern, False otherwise.
        """
        return True if re.match(self.pattern, value) else False


class Timestamp(FieldType):
    """Field type for timestamp validation and parsing

    Validates timestamp fields according to a specified format string and provides
    functionality to convert valid timestamps to ISO format for internal processing.
    """

    def __init__(self, name: str, timestamp_format: str):
        super().__init__(name)
        self.timestamp_format = timestamp_format

    def validate(self, value) -> bool:
        """Validates the input value against the configured timestamp format.

        Args:
            value: The timestamp string to be validated.

        Returns:
            True if the value matches the timestamp format, False otherwise.
        """
        try:
            datetime.datetime.strptime(value, self.timestamp_format)
        except ValueError:
            return False

        return True

    def get_timestamp_as_str(self, value) -> str:
        """Converts a valid timestamp to ISO format string.

        Args:
            value: Correctly formatted timestamp according to self.timestamp_format.

        Returns:
            ISO formatted timestamp string for internal processing.
        """
        return str(datetime.datetime.strptime(value, self.timestamp_format).isoformat())


class IpAddress(FieldType):
    """Field type for IP address validation

    Validates both IPv4 and IPv6 addresses using the utility validation functions.
    No additional configuration parameters are required beyond the field name.
    """

    def __init__(self, name):
        super().__init__(name)

    def validate(self, value) -> bool:
        """Validates the input value as a valid IP address.

        Args:
            value: The IP address string to be validated.

        Returns:
            True if the value is a valid IPv4 or IPv6 address, False otherwise.
        """
        try:
            validate_host(value)
        except ValueError:
            return False

        return True


class ListItem(FieldType):
    """Field type for list-based validation with optional relevance filtering

    Validates field values against an allowed list and optionally defines which values
    are considered relevant for filtering in later pipeline stages. The allowed_list
    contains all valid values, while the optional relevant_list defines a subset
    used for relevance-based filtering in the Log Filtering stage.
    """

    def __init__(self, name: str, allowed_list: list, relevant_list: list):
        super().__init__(name)
        self.allowed_list = allowed_list

        if relevant_list and not all(e in allowed_list for e in relevant_list):
            raise ValueError("Relevant types are not allowed types")

        self.relevant_list = relevant_list

    def validate(self, value) -> bool:
        """Validates the input value against the allowed list.

        Args:
            value: The value to be validated.

        Returns:
            True if the value is in the allowed_list, False otherwise.
        """
        return True if value in self.allowed_list else False

    def check_relevance(self, value) -> bool:
        """Checks if the given value is considered relevant for filtering.

        Args:
            value: Value to be checked for relevance.

        Returns:
            True if the value is relevant (in relevant_list or if no relevant_list is defined), False otherwise.
        """
        if self.relevant_list:
            return True if value in self.relevant_list else False

        return True


class LoglineHandler:
    """Main handler for logline validation and processing

    Manages the configuration-based validation of loglines according to the format
    specified in the configuration file. Provides validation, field extraction,
    and relevance checking functionality for the log processing pipeline.
    """

    def __init__(self):
        self.instances_by_name = {}
        self.instances_by_position = {}
        self.number_of_fields = 0

        for field in LOGLINE_FIELDS:
            instance = self._create_instance_from_list_entry(field)

            if instance.name in FORBIDDEN_FIELD_NAMES:
                raise ValueError(
                    f"Forbidden field name included. These fields are used internally "
                    f"and cannot be used as names: {FORBIDDEN_FIELD_NAMES}"
                )

            if self.instances_by_name.get(instance.name):
                raise ValueError("Multiple fields with same name")

            self.instances_by_position[self.number_of_fields] = instance
            self.instances_by_name[instance.name] = instance
            self.number_of_fields += 1

        for required_field in REQUIRED_FIELDS:
            if required_field not in self.instances_by_name:
                raise ValueError("Not all needed fields are set in the configuration")

        if self.number_of_fields == 0:
            raise ValueError("No fields configured")

    def validate_logline(self, logline: str) -> bool:
        """Validates a complete logline according to the configured format.

        Checks if the number of fields is correct and validates each field using
        the appropriate field type validator. Provides detailed error logging
        with visual indicators for incorrect fields.

        Args:
            logline (str): Logline string to be validated.

        Returns:
            True if the logline contains correct fields in the configured format, False otherwise.
        """
        parts = logline.split()
        number_of_entries = len(parts)

        # check number of entries
        if number_of_entries != self.number_of_fields:
            logger.warning(
                f"Logline contains {number_of_entries} value(s), not {self.number_of_fields}."
            )
            return False

        valid_values = []
        for i in range(self.number_of_fields):
            valid_values.append(self.instances_by_position.get(i).validate(parts[i]))

        if not all(valid_values):
            # handle logging
            error_line = len("[yyyy-mm-dd hh:mm:ss, WARNING] ") * " "
            error_line += len("Incorrect logline: ") * " "

            for i in range(self.number_of_fields):
                if valid_values[i]:
                    error_line += len(parts[i]) * " "  # keep all valid fields unchanged
                else:
                    error_line += len(parts[i]) * "^"  # underline all wrong fields
                error_line += " "

            logger.warning(f"Incorrect logline: {logline}\n{error_line}")
            return False

        return True

    def __get_fields_as_json(self, logline: str) -> dict:
        """Extracts fields from a logline and returns them as a dictionary.

        Parses the logline into individual fields and creates a dictionary with
        field names as keys and field values as values. Handles timestamp conversion
        to ISO format for internal processing. Does not perform validation.

        Args:
            logline (str): Logline to extract fields from.

        Returns:
            Dictionary with field names as keys and field values as values.
        """
        parts = logline.split()
        return_dict = {}

        for i in range(self.number_of_fields):
            if not isinstance(self.instances_by_position[i], Timestamp):
                return_dict[self.instances_by_position[i].name] = parts[i]
            else:
                return_dict[self.instances_by_position[i].name] = (
                    self.instances_by_position[i].get_timestamp_as_str(parts[i])
                )

        return return_dict.copy()

    def validate_logline_and_get_fields_as_json(self, logline: str) -> dict:
        """Validates a logline and returns the fields as a dictionary.

        Combines validation and field extraction in a single operation.
        First validates the logline format, then extracts and returns the fields.

        Args:
            logline (str): Logline string to be validated and parsed.

        Returns:
            Dictionary with field names as keys and field values as values.

        Raises:
            ValueError: If logline validation fails.
        """
        if not self.validate_logline(logline):
            raise ValueError("Incorrect logline, validation unsuccessful")

        return self.__get_fields_as_json(logline)

    def check_relevance(self, logline_dict: dict) -> bool:
        """Checks if a logline is relevant based on configured relevance criteria.

        Iterates through all ListItem fields and checks their relevance using
        the check_relevance method. A logline is considered relevant only if
        all ListItem fields pass their relevance checks.

        Args:
            logline_dict (dict): Logline fields as dictionary to be checked for relevance.

        Returns:
            True if the logline is relevant according to all configured criteria, False otherwise.
        """
        relevant = True

        for i in self.instances_by_position:
            current_instance = self.instances_by_position[i]
            if isinstance(current_instance, ListItem):
                if not current_instance.check_relevance(
                    logline_dict[current_instance.name]
                ):
                    relevant = False
                    break

        return relevant

    @staticmethod
    def _create_instance_from_list_entry(field_list: list):
        """Creates a field type instance from configuration list entry.

        Parses the configuration format and creates the appropriate field type instance
        based on the specified class name and parameters. Supports RegEx, Timestamp,
        ListItem, and IpAddress field types with their respective parameter requirements.

        Args:
            field_list (list): Configuration list containing field name, type, and parameters.

        Returns:
            Field type instance configured according to the specification.

        Raises:
            ValueError: If the field configuration is invalid or unsupported.
        """
        len_of_field_list = len(field_list)

        if len_of_field_list < 2 or not isinstance(field_list[0], str):
            raise ValueError("Invalid field list or field name")

        name, cls_name = field_list[0], field_list[1]

        try:
            cls = globals()[cls_name]
        except KeyError:
            raise ValueError(f"Class '{cls_name}' not found")

        if cls_name == "RegEx":
            if len_of_field_list != 3 or not isinstance(field_list[2], str):
                raise ValueError("Invalid RegEx parameters")
            instance = cls(name=name, pattern=field_list[2])

        elif cls_name == "Timestamp":
            if len_of_field_list != 3 or not isinstance(field_list[2], str):
                raise ValueError("Invalid Timestamp parameters")
            instance = cls(name=name, timestamp_format=field_list[2])

        elif cls_name == "ListItem":
            if len_of_field_list not in [3, 4] or not isinstance(field_list[2], list):
                raise ValueError("Invalid ListItem parameters")

            relevant_list = field_list[3] if len_of_field_list == 4 else None
            instance = cls(
                name=name, allowed_list=field_list[2], relevant_list=relevant_list
            )

        elif cls_name == "IpAddress":
            if len_of_field_list != 2:
                raise ValueError("Invalid IpAddress parameters")

            instance = cls(name=name)

        else:
            raise ValueError(f"Unsupported class '{cls_name}'")

        return instance
