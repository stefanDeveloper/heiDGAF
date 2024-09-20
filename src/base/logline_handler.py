import logging
import re

from src.base.log_config import setup_logging
from src.base.utils import setup_config, validate_host

setup_logging()
logger = logging.getLogger(__name__)

CONFIG = setup_config()

REQUIRED_FIELDS = ["timestamp", "status_code", "client_ip", "record_type"]


class FieldType:
    def __init__(self, name: str):
        self.name = name

    def validate(self, value) -> bool:
        raise NotImplementedError


class RegEx(FieldType):
    def __init__(self, name: str, pattern: str):
        super().__init__(name)
        self.pattern = re.compile(r"{}".format(pattern))

    def validate(self, value) -> bool:
        return True if re.match(self.pattern, value) else False


class IpAddress(FieldType):
    def __init__(self, name):
        super().__init__(name)

    def validate(self, value) -> bool:
        try:
            validate_host(value)
        except ValueError:
            return False

        return True


class ListItem(FieldType):
    def __init__(self, name: str, allowed_list: list, relevant_list: list):
        super().__init__(name)
        self.allowed_list = allowed_list

        if relevant_list and not all(e in allowed_list for e in relevant_list):
            raise ValueError('Relevant types are not allowed types')

        self.relevant_list = relevant_list

    def validate(self, value) -> bool:
        return True if value in self.allowed_list else False

    # TODO: Add method to check if value in relevant list


# TODO: Test
class LoglineHandler:
    def __init__(self):
        self.instances_by_name = {}
        self.instances_by_position = {}
        self.number_of_fields = 0

        for field in CONFIG['loglines']['fields']:
            instance = self._create_instance_from_list_entry(field)

            if self.instances_by_name.get(instance.name):
                raise ValueError("Multiple fields with same name")

            self.instances_by_position[self.number_of_fields] = instance
            self.instances_by_name[instance.name] = instance
            self.number_of_fields += 1

        for required_field in REQUIRED_FIELDS:
            if required_field not in self.instances_by_name:
                raise ValueError("Not all needed fields are set in the configuration")

    def validate_logline(self, logline: str) -> bool:
        parts = logline.split()

        # check number of entries
        if len(parts) != self.number_of_fields:
            logger.warning(
                f"Logline contains {len(parts)} value(s), not {self.number_of_fields}.")
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
                    error_line += len(parts[i]) * " "  # keep all valid fields normal
                else:
                    error_line += len(parts[i]) * "^"  # underline all wrong fields
                error_line += " "

            logger.warning(f"Incorrect logline: {logline}\n{error_line}")
            return False

        return True

    def validate_logline_and_get_fields_as_json(self, logline: str) -> dict:
        if not self.validate_logline(logline):
            raise ValueError("Incorrect logline, validation unsuccessful")

        parts = logline.split()
        return_dict = {}

        for i in range(self.number_of_fields):
            return_dict[self.instances_by_position[i].name] = parts[i]

        return return_dict.copy()

    def _create_instance_from_list_entry(self, field_list: list):
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
            instance = cls(name=name, allowed_list=field_list[2], relevant_list=relevant_list)

        elif cls_name == "IpAddress":
            if len_of_field_list != 2:
                raise ValueError("Invalid IpAddress parameters")

            instance = cls(name=name)

        else:
            raise ValueError(f"Unsupported class '{cls_name}'")

        return instance


sut = Logline()
