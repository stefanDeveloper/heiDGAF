import re

from src.base.utils import setup_config, validate_host

CONFIG = setup_config()


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
            return True
        except ValueError:
            return False


class ListItem(FieldType):
    def __init__(self, name: str, allowed_list: list, relevant_list: list):
        super().__init__(name)
        self.allowed_list = allowed_list

        if relevant_list and not all(e in allowed_list for e in relevant_list):
            raise ValueError

        self.relevant_list = relevant_list

    def validate(self, value) -> bool:
        return True if value in self.allowed_list else False


class Logline:
    def __init__(self):
        self.instances = {}

        for field in CONFIG['loglines']['fields']:
            instance = self._create_instance_from_list_entry(field)

            if self.instances.get(instance.name):
                raise ValueError("Multiple fields with same name")

            self.instances[instance.name] = instance

        print(self.instances)

    def validate_fields(self, logline):
        pass

    def _create_instance_from_list_entry(self, field_list: list):
        if len(field_list) < 2 or not isinstance(field_list[0], str):
            raise ValueError("Invalid field list or field name")

        name, cls_name = field_list[0], field_list[1]

        try:
            cls = globals()[cls_name]
        except KeyError:
            raise ValueError(f"Class '{cls_name}' not found")

        if cls_name == "RegEx":
            if len(field_list) != 3 or not isinstance(field_list[2], str):
                raise ValueError("Invalid RegEx parameters")
            instance = cls(name=name, pattern=field_list[2])

        elif cls_name == "ListItem":
            if len(field_list) not in [3, 4] or not isinstance(field_list[2], list):
                raise ValueError("Invalid ListItem parameters")

            relevant_list = field_list[3] if len(field_list) == 4 else None
            instance = cls(name=name, allowed_list=field_list[2], relevant_list=relevant_list)

        elif cls_name == "IpAddress":
            if len(field_list) != 2:
                raise ValueError("Invalid IpAddress parameters")

            instance = cls(name=name)

        else:
            raise ValueError(f"Unsupported class '{cls_name}'")

        return instance


sut = Logline()
