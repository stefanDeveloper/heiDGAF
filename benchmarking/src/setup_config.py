import os
import sys

import yaml

sys.path.append(os.getcwd())
from src.base.log_config import get_logger
from src.base.utils import setup_config

logger = get_logger()
config = setup_config()

CONFIG_FILEPATH = os.path.join(os.path.dirname(__file__), "../benchmark_config.yaml")


def setup_config():
    """
    Loads the configuration data from the configuration file and returns it as the corresponding Python object.

    Returns:
         Configuration data as corresponding Python object

    Raises:
        FileNotFoundError: Configuration file could not be opened
    """
    try:
        logger.debug(
            f"Opening benchmark test configuration file at {CONFIG_FILEPATH}..."
        )
        with open(CONFIG_FILEPATH, "r") as file:
            configuration = yaml.safe_load(file)
    except FileNotFoundError:
        logger.critical(f"File {CONFIG_FILEPATH} does not exist. Aborting...")
        raise

    logger.debug("Configuration file successfully opened and information returned.")
    return configuration
