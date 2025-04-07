import os.path

import pandas as pd


class InputReader:
    """Reads data from an input file."""

    def __init__(self, file_suffix: str):
        self.file_suffix = file_suffix

    @staticmethod
    def __file_exists_and_is_readable(file_name: str) -> bool:
        return os.path.isfile(file_name) and os.access(file_name, os.R_OK)

    def __file_type_is_correct(self, file_name: str) -> bool:
        return file_name.lower().endswith(self.file_suffix)


class CSVInputReader(InputReader):
    """Reads data from a CSV input file."""

    def __init__(self):
        super().__init__(".csv")

    def read(self, input_file_name: str, parse_dates_col_name: str) -> pd.DataFrame:
        """Reads the data from the file and returns the resulting data frame.

        Args:
            input_file_name (str): Name or path of the input CSV file
            parse_dates_col_name (None | str): Name of the column to be parsed as data or time

        Returns:
            pandas.Dataframe of the data imported from the file
        """
        ## Check if input_file_name exists and is a CSV file

        ## Import data into pd.DataFrame

        pass
