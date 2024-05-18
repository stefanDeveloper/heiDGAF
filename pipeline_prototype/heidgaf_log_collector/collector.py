import socket

from pipeline_prototype.heidgaf_log_collector import utils


class LogCollector:
    server_host = None
    server_port = None

    def __init__(self, server_host, server_port):
        self.server_host = utils.validate_host(server_host)
        self.server_port = utils.validate_port(server_port)

    def fetch_logline(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as self.client_socket:
            self.client_socket.connect((str(self.server_host), self.server_port))
            while True:
                data = self.client_socket.recv(1024)
                if not data:
                    break
                logline = data.decode('utf-8')
                print(f"Received logline: {logline}")


collector = LogCollector("127.0.0.1", 9999)
collector.fetch_logline()
