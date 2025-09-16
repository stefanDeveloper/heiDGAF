import re

from src.base.log_config import get_logger
from src.base.utils import setup_config, ValidationUtils

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
    """
    Base class for types of fields.
    """

    def __init__(self, name: str):
        self.name = name

    def validate(self, value) -> bool:
        """
        Validates the input value. Implementation in inheriting classes.

        Args:
            value: The value to be validated

        Raises:
            NotImplementedError
        """
        raise NotImplementedError


class RegEx(FieldType):
    """
    A :cls:`RegEx` object takes a name, and a pattern, which a value needs to have the format of.
    """

    def __init__(self, name: str, pattern: str):
        super().__init__(name)
        self.pattern = re.compile(r"{}".format(pattern))

    def validate(self, value) -> bool:
        """
        Validates the input value.

        Args:
            value: The value to be validated

        Returns:
            True if the value is valid, False otherwise
        """
        return True if re.match(self.pattern, value) else False


class IpAddress(FieldType):
    """
    An :cls:`IpAddress` object takes only a name. It is used for IP addresses, and checks in the :meth:`validate` method
    if the value is a correct IP address.
    """

    def __init__(self, name):
        super().__init__(name)

    def validate(self, value) -> bool:
        """
        Validates the input value.

        Args:
            value: The value to be validated

        Returns:
            True if the value is valid, False otherwise
        """
        try:
            ValidationUtils.validate_host(value)
        except ValueError:
            return False

        return True


class ListItem(FieldType):
    """
    A :cls:`ListItem` object takes a name, and two lists: The
    ``allowed_list`` contains all values, that the :cls:`ListItem` is allowed to have, and therefore are not sorted out.
    The ``relevant_list`` must contain fields that are also in ``allowed_list`` and that are relevant for further
    inspection. These are filtered in the Prefilter stage.
    """

    def __init__(self, name: str, allowed_list: list, relevant_list: list):
        super().__init__(name)
        self.allowed_list = allowed_list

        if relevant_list and not all(e in allowed_list for e in relevant_list):
            raise ValueError("Relevant types are not allowed types")

        self.relevant_list = relevant_list

    def validate(self, value) -> bool:
        """
        Validates the input value.

        Args:
            value: The value to be validated

        Returns:
            True if the value is valid, False otherwise
        """
        return True if value in self.allowed_list else False

    def check_relevance(self, value) -> bool:
        """
        Checks if the given value is a relevant value.

        Args:
            value: Value to be checked for relevance

        Returns:
            True if the value is relevant, else False
        """
        if self.relevant_list:
            return True if value in self.relevant_list else False

        return True


class LoglineHandler:
    """
    Stores the configuration format of loglines and can be used to validate a given logline, i.e. checks if the given
    logline has the format given in the configuration. Can also return the validated logline as dictionary.
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
        """
        Validates the given input logline by checking if the number of fields is correct as well as all the fields, by
        calling the :meth:`validate` method of each field. If the logline is incorrect, it shows an error with the
        incorrect fields being highlighted.

        Args:
            logline (str): Logline as string to be validated

        Returns:
            True if the logline contains correct fields in the configured format, False otherwise
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
        """
        Returns the fields of the given logline as dictionary, with the names of the fields as key, and the field value
        as value. Does not validate fields.

        Args:
            logline (str): Logline to get the fields from

        Returns:
            Dictionary of field names as keys and field values as value
        """
        parts = logline.split()
        return_dict = {}

        for i in range(self.number_of_fields):
            return_dict[self.instances_by_position[i].name] = parts[i]

        return return_dict.copy()

    def validate_logline_and_get_fields_as_json(self, logline: str) -> dict:
        """Validates the fields and returns them as dictionary, with the names of the fields as key, and the field
        value as value.

        Args:
            logline (str): Logline as string to be validated

        Returns:
            Dictionary of field names as keys and field values as value
        """
        if not self.validate_logline(logline):
            raise ValueError("Incorrect logline, validation unsuccessful")

        return self.__get_fields_as_json(logline)

    def check_relevance(self, logline_dict: dict) -> bool:
        """
        Checks if the given logline is relevant.

        Args:
            logline_dict (dict): Logline parts to be checked for relevance as dictionary

        Returns:
            True if the logline is relevant, else False
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
        """
        Extracts the information from the ``field_list`` to generate one instance of the specified type.

        Args:
            field_list (list): List of field name, type and additional fields

        Returns:
            Generated instance with the name, type and additional parameters given in the ``field_list``
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
