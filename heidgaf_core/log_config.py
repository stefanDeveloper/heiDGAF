import logging

import colorlog

DEBUG = True


def setup_logging():
    log_colors = {
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white',
    }

    # Formatter for INFO and WARNING levels
    simple_formatter = colorlog.ColoredFormatter(
        fmt='%(log_color)s[%(asctime)s, %(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        log_colors=log_colors
    )

    # Formatter for DEBUG, ERROR, and CRITICAL levels
    detailed_formatter = colorlog.ColoredFormatter(
        fmt='%(log_color)s%(asctime)s [%(levelname)s] %(message)s\n    ⤷ In %(module)s:%(lineno)d, %(funcName)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        log_colors=log_colors
    )

    class CustomHandler(logging.StreamHandler):
        def format(self, record):
            if record.levelno in (logging.INFO, logging.WARNING):
                return simple_formatter.format(record)
            return detailed_formatter.format(record)

    handler = CustomHandler()

    logger = logging.getLogger()

    if DEBUG:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    logger.addHandler(handler)

    if len(logger.handlers) > 1:
        logger.handlers = [handler]
