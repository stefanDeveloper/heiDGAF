import logging
import os
import socket
import sys
from time import sleep

sys.path.append(os.getcwd())
from src.base import utils
from src.base.log_config import setup_logging
from src.mock.log_generator import generate_dns_log_line

setup_logging()
logger = logging.getLogger(__name__)


class LogGenerator:
    server_host = None
    server_port = None

    def __init__(self, server_host, server_port):
        self.server_host = utils.validate_host(server_host)
        self.server_port = utils.validate_port(server_port)

    def send_logline(self, logline: str):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as self.client_socket:
            self.client_socket.connect((str(self.server_host), self.server_port))
            self.client_socket.send(logline.encode("utf-8"))
            logger.info(f"Sent {logline} to server")


if __name__ == "__main__":
    generator = LogGenerator("127.0.0.1", 9998)
    # while True:
    logline = generate_dns_log_line()
    generator.send_logline(logline)
    logger.info(f"Sent logline: {logline}")
    # sleep(0.1)
    with open("../../pipeline_prototype/loglines.txt", 'r', encoding='utf-8') as file:
        lines = file.readlines()

    for index, line in enumerate(lines, start=1):
        generator.send_logline(line)
