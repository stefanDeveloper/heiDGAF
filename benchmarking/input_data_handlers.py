import os
from abc import abstractmethod


class InputDataHandler:
    """Base class for handlers that manage data that was loaded from a file."""

    def __init__(self, input_data_file: str):
        self.file_suffix = None
        self.input_data_file = input_data_file

        self._verify_file()
        self.__load_data()

    def _verify_file(self):
        if not os.path.isfile(self.input_data_file):
            raise FileNotFoundError

        if not os.access(self.input_data_file, os.R_OK):
            raise PermissionError

        if self.file_suffix and not self.input_data_file.lower().endswith(
            self.file_suffix
        ):
            raise Exception(f"Wrong file type: Expected {self.file_suffix}")

    @abstractmethod
    def __load_data(self):
        raise NotImplementedError


class CSVInputDataHandler(InputDataHandler):
    """Handles data that is loaded from a CSV file."""

    def __init__(self, input_data_file: str):
        self.file_suffix = ".csv"
        super().__init__(input_data_file)

    def __load_data(self):
        pass
