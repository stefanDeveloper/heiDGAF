import inspect
import logging

import colorlog

DEBUG = False


def find_class_name():
    current_frame = inspect.currentframe()
    try:
        # Walk up the call stack to find the caller's frame
        for _ in range(3):  # Skip this function, logging function, and logger method
            if current_frame.f_back is None:
                return None
            current_frame = current_frame.f_back

        # Check if the frame is within a class method
        if 'self' in current_frame.f_locals:
            cls_name = current_frame.f_locals['self'].__class__.__name__

            return cls_name if cls_name != 'Logger' else None
        return None
    finally:
        del current_frame


class ClassNameLogRecord(logging.LogRecord):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.classname = find_class_name()


def setup_logging():
    logging.setLogRecordFactory(ClassNameLogRecord)

    log_colors = {
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white',
    }

    # Formatter for INFO level
    info_formatter = colorlog.ColoredFormatter(
        fmt='%(log_color)s%(asctime)s [INFO] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        log_colors=log_colors
    )

    # Formatter for WARNING level
    warning_formatter = colorlog.ColoredFormatter(
        fmt='%(log_color)s%(asctime)s [WARNING] %(message)s\n    ⤷ In %(module)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        log_colors=log_colors
    )

    # Formatter for DEBUG, ERROR, and CRITICAL levels
    detailed_formatter = colorlog.ColoredFormatter(
        fmt='%(log_color)s%(asctime)s [%(levelname)s] %(message)s\n    ⤷ In %(module)s%(classname)s.%(funcName)s:%('
            'lineno)d',
        datefmt='%Y-%m-%d %H:%M:%S',
        log_colors=log_colors
    )

    class CustomHandler(logging.StreamHandler):
        def format(self, record):
            if record.levelno == logging.INFO:
                return info_formatter.format(record)
            elif record.levelno == logging.WARNING:
                return warning_formatter.format(record)
            else:
                if record.classname:
                    record.classname = f".{record.classname}"
                else:
                    record.classname = ""
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
