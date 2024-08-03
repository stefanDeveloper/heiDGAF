import logging
import os
from datetime import datetime

import colorlog
import pytz
import yaml

CONFIG_FILEPATH = os.path.join(os.path.dirname(__file__), "../../config.yaml")


def setup_logging() -> None:
    """
    Sets up the logger with colored logging messages.

    Raises:
        FileNotFoundError: Configuration file could not be opened
    """
    try:
        with open(CONFIG_FILEPATH, "r") as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        raise

    log_colors = {
        "DEBUG": "cyan",
        "INFO": "green",
        "WARNING": "yellow",
        "ERROR": "red",
        "CRITICAL": "red,bg_white",
    }

    def current_time_with_timezone():
        timezone = pytz.timezone('Europe/Berlin')
        return datetime.now(timezone)

    class CustomFormatter(colorlog.ColoredFormatter):
        def formatTime(self, record, datefmt=None):
            dt = current_time_with_timezone()
            if datefmt:
                return dt.strftime(datefmt)
            return dt.isoformat()

    # Formatter für INFO und WARNING Levels
    simple_formatter = CustomFormatter(
        fmt="%(log_color)s[%(asctime)s, %(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors=log_colors,
    )

    # Formatter für DEBUG, ERROR und CRITICAL Levels
    detailed_formatter = CustomFormatter(
        fmt="%(log_color)s%(asctime)s [%(levelname)s] %(message)s\n    ⤷  In %(module)s:%(lineno)d, %(funcName)s",
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

    handler = CustomHandler()

    logger = logging.getLogger()

    if config["logging"]["debug"]:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    logger.addHandler(handler)

    if len(logger.handlers) > 1:
        logger.handlers = [handler]
