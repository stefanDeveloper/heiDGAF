import logging
import os
from typing import Dict, Any

import colorlog
import yaml

CONFIG_FILEPATH = os.path.join(os.path.dirname(__file__), "../../config.yaml")

# Global formatting for all loggers
log_colors = {
    "DEBUG": "cyan",
    "INFO": "green",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "red,bg_white",
}

# Formatter for INFO and WARNING levels
simple_formatter = colorlog.ColoredFormatter(
    fmt="%(log_color)s[%(asctime)s, %(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    log_colors=log_colors,
)

# Formatter for DEBUG, ERROR, and CRITICAL levels
detailed_formatter = colorlog.ColoredFormatter(
    fmt="%(log_color)s%(asctime)s [%(levelname)s] %(message)s\n    ⤷ In %(module)s:%(lineno)d, %(funcName)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    log_colors=log_colors,
)


class CustomHandler(logging.StreamHandler):
    """
    Handles the different styles of logging messages with respect to their level.
    """

    def format(self, record) -> str:
        """
        Formats the data with respect to the level. Uses the simple format for INFO and WARNING messages,
        for all other levels, the detailed format is used.

        Args:
            record: record to be formatted

        Returns:
            formatted logging info
        """
        if record.levelno in (logging.INFO, logging.WARNING):
            return simple_formatter.format(record)
        return detailed_formatter.format(record)


def load_config() -> Dict[str, Any]:
    """Loads the configuration file."""
    try:
        with open(CONFIG_FILEPATH, "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        raise


def get_logger(module_name: str = "base") -> logging.Logger:
    """
    Creates or retrieves a logger for a specific module.

    Args:
        module_name (str): Name of the module (as defined in config.yaml)

    Returns:
        Configured logger for the module
    """
    config = load_config()
    logger = logging.getLogger(module_name)

    if logger.hasHandlers():
        logger.handlers.clear()

    handler = CustomHandler()
    logger.addHandler(handler)

    # Default to base debug setting
    debug_enabled = config["logging"]["base"]["debug"]

    # Override with module-specific setting if it exists
    if module_name in config["logging"]["modules"]:
        debug_enabled = config["logging"]["modules"][module_name]["debug"]

    logger.setLevel(logging.DEBUG if debug_enabled else logging.INFO)

    return logger
