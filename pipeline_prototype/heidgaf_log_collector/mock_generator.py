import logging
import os  # needed for Terminal execution
import socket
import sys  # needed for Terminal execution

sys.path.append(os.getcwd())  # needed for Terminal execution
from pipeline_prototype.heidgaf_log_collector import utils
from pipeline_prototype.logging_config import setup_logging
from pipeline_prototype.heidgaf_log_collector.log_generator import generate_dns_log_line

setup_logging()
logger = logging.getLogger(__name__)


def generate_random_logline():
    return generate_dns_log_line()


class LogGenerator:
    server_host = None
    server_port = None

    def __init__(self, server_host, server_port):
        self.server_host = utils.validate_host(server_host)
        self.server_port = utils.validate_port(server_port)

    def send_logline(self, logline: str):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as self.client_socket:
            self.client_socket.connect((str(self.server_host), self.server_port))
            self.client_socket.send(logline.encode('utf-8'))
            logger.info(f"Sent {logline} to server")


if __name__ == "__main__":
    generator = LogGenerator("127.0.0.1", 9999)
    generator.send_logline(generate_random_logline())
