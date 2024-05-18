import logging
import socket

from pipeline_prototype.heidgaf_log_collector import utils
from pipeline_prototype.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


class LogGenerator:
    server_host = None
    server_port = None

    def __init__(self, server_host, server_port):
        self.server_host = utils.validate_host(server_host)
        self.server_port = utils.validate_port(server_port)

    def send_logline(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as self.client_socket:
            self.client_socket.connect((str(self.server_host), self.server_port))
            self.client_socket.send("Test element".encode('utf-8'))
            logger.info("Sent Test element to server")


not_a_real_connector = LogGenerator("127.0.0.1", 9999)
not_a_real_connector.send_logline()
