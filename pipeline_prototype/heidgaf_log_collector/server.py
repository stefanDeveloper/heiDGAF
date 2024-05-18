import asyncio

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

    async def open(self):
        active_server = await asyncio.start_server(
            self.handle_connection,
            str(self.host),
            self.port
        )
        self.active = True
        print(f"LogServer running on {self.host}:{self.port}")  # TODO: Change to logging line

        try:
            await active_server.serve_forever()
        except KeyboardInterrupt:
            pass

        active_server.close()
        await active_server.wait_closed()

    async def handle_connection(self, reader, writer):
        if self.number_of_connections <= MAX_NUMBER_OF_CONNECTIONS:
            self.number_of_connections += 1
            client_address = writer.get_extra_info('peername')
            print(f"Connection from {client_address} accepted.")  # TODO: Change to logging line

            try:
                await self.send_logline(writer, self.get_next_logline())
            except asyncio.CancelledError:
                pass
            finally:
                print(f"Connection to {client_address} closed.")  # TODO: Change to logging line
                writer.close()
                await writer.wait_closed()
                self.number_of_connections -= 1
        else:
            client_address = writer.get_extra_info('peername')
            print(
                f"Client connection to {client_address} denied. Max number of connections reached!"
            )  # TODO: Change to logging line
            writer.close()
            await writer.wait_closed()

    async def send_logline(self, writer, logline):
        writer.write(logline.encode('utf-8'))
        await writer.drain()
        print(f"Logline sent: {logline}")  # TODO: Change to logging line

    def get_next_logline(self) -> str:
        # TODO: Implement, currently only mock method
        return "this is a mock logline"

    def receive_data(self):
        pass


server = LogServer("127.0.0.1", 9999)
asyncio.run(server.open())
