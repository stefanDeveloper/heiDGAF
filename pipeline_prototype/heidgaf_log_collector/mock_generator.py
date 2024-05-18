import socket

from pipeline_prototype.heidgaf_log_collector import utils


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
            print("Added Test element to queue")


not_a_real_connector = LogGenerator("127.0.0.1", 9999)
not_a_real_connector.send_logline()
