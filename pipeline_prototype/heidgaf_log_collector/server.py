import socket

from pipeline_prototype.heidgaf_log_collector import utils

MAX_NUMBER_OF_CONNECTIONS = 1


class Server:
    host = None
    port = None
    socket = None

    def __init__(self, host: str, port: int):
        self.host = utils.validate_host(host)
        self.port = utils.validate_port(port)

    def open_socket(self):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # TODO: Add log message for opening the socket
            self.socket.bind((self.host, self.port))
            self.socket.listen(MAX_NUMBER_OF_CONNECTIONS)
        except socket.error as e:
            raise RuntimeError(f"Failed to open socket: {e}")

    def close_socket(self):
        pass

    def receive_data(self):
        pass
