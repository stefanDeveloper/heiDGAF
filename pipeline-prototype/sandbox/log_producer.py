import socket


def main():
    host = 'localhost'
    port = 9999

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)

    print(f"Server läuft auf {host}:{port}")

    while True:
        client_socket, addr = server_socket.accept()
        print(f"Neue Verbindung von {addr}")

        data = client_socket.recv(1024)
        if not data:
            break

        process_data(data)

        client_socket.close()


def process_data(data):
    print("Empfangene Daten:", data.decode())


if __name__ == "__main__":
    main()
