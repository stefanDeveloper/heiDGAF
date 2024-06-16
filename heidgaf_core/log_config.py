import logging

import colorlog

DEBUG = False


def setup_logging():
    log_colors = {
        'DEBUG': 'cyan',
        'INFO': 'white',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white',
    }

    formatter = colorlog.ColoredFormatter(
        fmt='%(log_color)s[%(asctime)s, %(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        log_colors=log_colors
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger()

    if DEBUG:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    logger.addHandler(handler)

    if len(logger.handlers) > 1:
        logger.handlers = [handler]
