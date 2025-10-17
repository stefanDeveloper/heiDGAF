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
    """Custom logging handler that applies different formatting based on log level

    Provides level-specific message formatting where INFO and WARNING messages
    use a simplified format, while DEBUG, ERROR, and CRITICAL messages include
    detailed context information such as module name, line number, and function name.
    """

    def format(self, record) -> str:
        """Format log records with level-appropriate detail.

        Applies simple formatting for INFO and WARNING messages, and detailed
        formatting (including module, line number, and function name) for all
        other log levels.

        Args:
            record: The log record to format.

        Returns:
            str: Formatted log message string.
        """
        if record.levelno in (logging.INFO, logging.WARNING):
            return simple_formatter.format(record)
        return detailed_formatter.format(record)


def load_config() -> Dict[str, Any]:
    """Load the application configuration from the YAML configuration file.

    Returns:
        Dict[str, Any]: Parsed configuration data as a dictionary.

    Raises:
        FileNotFoundError: If the configuration file cannot be found at the expected path.
        yaml.YAMLError: If the configuration file contains invalid YAML syntax.
    """
    try:
        with open(CONFIG_FILEPATH, "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        raise


def get_logger(module_name: str = "base") -> logging.Logger:
    """Create or retrieve a configured logger for a specific module.

    Sets up a logger with custom formatting and debug level configuration
    based on the module-specific settings in config.yaml. If no module-specific
    configuration exists, falls back to the base module settings.

    Args:
        module_name (str): Name of the module to create a logger for.
                           Must match a module defined in config.yaml ``logging.modules``.
                           Default: "base".

    Returns:
        logging.Logger: Configured logger instance for the specified module.

    Raises:
        FileNotFoundError: If the configuration file cannot be loaded.
        KeyError: If the configuration structure is invalid.
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
