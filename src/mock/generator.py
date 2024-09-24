import logging
import os
import socket
import sys
import time

sys.path.append(os.getcwd())
from src.base import utils
from src.base.log_config import setup_logging
from src.mock.log_generator import generate_dns_log_line

setup_logging()
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((str("127.0.0.1"), 9998))
        while True:
            for i in range(0, 10):
                logline = generate_dns_log_line()
                client_socket.send(logline.encode("utf-8"))
                logger.info(f"Sent logline: {logline}")
            time.sleep(0.1)
