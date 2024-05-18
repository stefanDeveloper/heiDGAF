import socket

from pipeline_prototype.heidgaf_log_collector import utils

MAX_NUMBER_OF_CONNECTIONS = 5


class LogServer:
    host = None
    port = None
    socket = None
    number_of_connections = 0
    active = False

    def __init__(self, host: str, port: int) -> None:
        self.host = utils.validate_host(host)
        self.port = utils.validate_port(port)

    def open(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as self.server_socket:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen()
            self.active = True
            print(f"LogServer running on {self.host}:{self.port}")  # TODO: Change to logging line

            while self.active:
                if self.number_of_connections <= MAX_NUMBER_OF_CONNECTIONS:
                    client_socket, client_address = self.server_socket.accept()
                    self.handle_connection(client_socket, client_address)
                else:
                    print(
                        f"Client connection to {client_address} denied. Max number of connections reached!"
                    )  # TODO: Change to logging line

    def handle_connection(self, client_socket, client_address):
        if self.active:
            with client_socket:
                print(f"Client connection to {client_address} accepted")  # TODO: Change to logging line
                # TODO: Add if statement that checks if PQ is not empty
                self.send_logline(client_socket, self.get_next_logline())

    def send_logline(self, client_socket, logline: str):
        client_socket.sendall(logline.encode('utf-8'))
        print(f"Sent logline: {logline}")  # TODO: Change to logging line

    def get_next_logline(self) -> str:
        # TODO: Implement, currently only mock method
        return "this is a mock logline"

    def receive_data(self):
        pass

    def close(self):
        self.active = False
