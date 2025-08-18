import datetime
import re
import json
from src.base.log_config import get_logger
from src.base.utils import setup_config, validate_host

logger = get_logger()

CONFIG = setup_config()

REQUIRED_FIELDS = ["ts", "src_ip"]
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
        return True if re.match(self.pattern, str(value)) else False


class Timestamp(FieldType):
    """
    A :cls:`Timestamp` object takes a name, and a timestamp format, which a value needs to have.
    """

    def __init__(self, name: str, timestamp_format: str):
        super().__init__(name)
        self.timestamp_format = timestamp_format

    def validate(self, value) -> bool:
        """
        Validates the input value.

        Args:
            value: The value to be validated

        Returns:
            True if the value is valid, False otherwise
        """
        try:
            datetime.datetime.strptime(value, self.timestamp_format)
        except ValueError:
            return False

        return True

    def get_timestamp_as_str(self, value) -> str:
        """
        Returns the timestamp as string for a given timestamp with valid format.

        Args:
            value: Correctly formatted timestamp according to self.timestamp_format

        Returns:
            String of the given timestamp with standard format
        """
        return str(datetime.datetime.strptime(value, self.timestamp_format).isoformat())


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
            validate_host(value)
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


class RelevanceHandler:
    """
    Handler class to check the relevance of a given logline. Loads the appropriate child method by the name, configured
    in the config.yaml at the ``log_filtering`` section from the ``relevance_method`` attribute. 
    """
    def __init__(self, log_configuration_instances):
        self.log_configuration_instances = log_configuration_instances

    def check_relevance(self, function_name: str, logline_dict: dict) -> bool:
        """
            wrapper function to get the appropriate relevance function by name.
            
            Args: 
                function_name (str): The name of the relevance_method to import
                logline_dict (dict): The dictionary version of a logline
                
            Returns:
                True, if the logline is relevant according to the relevance function, else False
        """
        is_relevant = False
        try:
            is_relevant = getattr(self, function_name)(logline_dict)
        except AttributeError as e:
            logger.error(f"Function {function_name} is not implemented!")
            raise Exception(f"Function {function_name} is not implemented!")
        return is_relevant

    def check_dga_relevance(self, logline_dict: dict) -> bool:
        """
        Method to check if a given logline is relevant for a dga analysis.
                    
        Args: 
            logline_dict (dict): The dictionary version of a logline
            
        Returns:
            True, if the logline is relevant according to the relevance function, else False
        """
        relevant = True
        for _, instance_configuartion in self.log_configuration_instances.items():
            if isinstance(instance_configuartion, ListItem):
                if instance_configuartion.relevant_list:
                    relevant = (
                        logline_dict[instance_configuartion.name]
                        in instance_configuartion.relevant_list
                    )
                    if not relevant:
                        return relevant
        return relevant

    def no_relevance_check(self, logline_dict: dict) -> bool:
        """
            Skip the relevance check by always returning True

        Args: 
            logline_dict (dict): The dictionary version of a logline
            
        Returns:
            Always returns True (all lines are relevant)
        """
        return True


class LoglineHandler:
    """
    Stores the configuration format of loglines and can be used to validate a given logline, i.e. checks if the given
    logline has the format given in the configuration. Can also return the validated logline as dictionary.
    """

    def __init__(self, validation_config: list):
        """
        Check all existing log configurations for validity.
        
        Args:
            validation_config (list): A list containing the configured attributes a given logline needs to hold. Otherwise it gets discarded
        """
        self.logformats = validation_config
        log_configuration_instances = {}
        if not validation_config:
            raise ValueError("No fields configured")
        for log_config_item in validation_config:
            instance = self._create_instance_from_list_entry(log_config_item)
            if instance.name in FORBIDDEN_FIELD_NAMES:
                raise ValueError(
                    f"Forbidden field name included. These fields are used internally "
                    f"and cannot be used as names: {FORBIDDEN_FIELD_NAMES}"
                )
            if log_configuration_instances.get(instance.name):
                raise ValueError("Multiple fields with same name")
            else:
                log_configuration_instances[instance.name] = instance
        for required_field in REQUIRED_FIELDS:

            if required_field not in log_configuration_instances.keys():
                raise ValueError("Not all needed fields are set in the configuration")

        self.relvance_handler = RelevanceHandler(
            log_configuration_instances=log_configuration_instances
        )

    def validate_logline(self, logline: str) -> bool:
        """
        Validates the given input logline by checking if the fields presented are corresponding to a given logformat of a protocol.
        Calls the :meth:`validate` method for each field. If the logline is incorrect, it shows an error with the
        incorrect fields being highlighted.

        Args:
            logline (str): Logline as string to be validated

        Returns:
            True if the logline contains correct fields in the configured format, False otherwise
        """
        logline = json.loads(logline)
        valid_values = []
        invalid_value_names = []
        for log_config_item in self.logformats:
            # by convention the first item is always the key present in a logline
            log_line_property_key = log_config_item[0]
            instance = self._create_instance_from_list_entry(log_config_item)
            try:
                is_value_valid = instance.validate(logline.get(log_line_property_key))
                valid_values.append(is_value_valid)
                if not is_value_valid:
                    invalid_value_names.append(log_line_property_key)
            except:
                logger.error(
                    f"line {logline} does not contain the specified field of {log_line_property_key}"
                )
        if all(valid_values):
            return True
        return False

    def __get_fields_as_json(self, logline: str) -> dict:
        """
        Returns the fields of the given logline as dictionary, with the names of the fields as key, and the field value
        as value. Does not validate fields.

        Args:
            logline (str): Logline to get the fields from

        Returns:
            Dictionary of field names as keys and field values as value
        """
        return json.loads(logline)

    def validate_logline_and_get_fields_as_json(self, logline: str) -> dict:
        """Validates the fields and returns them as dictionary, with the names of the fields as key, and the field
        value as value.

        Args:
            logline (dict): Logline parsed from zeek

        Returns:
            Dictionary of field names as keys and field values as value
        """
        if not self.validate_logline(logline):
            raise ValueError("Incorrect logline, validation unsuccessful")
        return self.__get_fields_as_json(logline)

    def check_relevance(self, logline_dict: dict, function_name: str) -> bool:
        """
        Checks if the given logline is relevant.

        Args:
            logline_dict (dict): Logline parts to be checked for relevance as dictionary
            function_name (str): A string that points to the relevance function to use

        Returns:
            Propagates the bool from the subordinate relevance method
        """
        return self.relvance_handler.check_relevance(
            function_name=function_name, logline_dict=logline_dict
        )

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
