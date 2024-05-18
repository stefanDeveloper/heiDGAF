import asyncio
import logging
import queue

from pipeline_prototype.heidgaf_log_collector import utils
from pipeline_prototype.logging_config import setup_logging

MAX_NUMBER_OF_CONNECTIONS = 5

setup_logging()
logger = logging.getLogger(__name__)


class LogServer:
    host = None
    send_port = None
    receive_port = None
    socket = None
    number_of_connections = 0

    def __init__(self, host: str, send_port: int, receive_port: int) -> None:
        self.host = utils.validate_host(host)
        self.send_port = utils.validate_port(send_port)
        self.receive_port = utils.validate_port(receive_port)
        self.data_queue = queue.Queue()

    async def open(self):
        send_server = await asyncio.start_server(
            self.handle_connection,
            str(self.host),
            self.send_port
        )
        receive_server = await asyncio.start_server(
            self.receive_logline,
            str(self.host),
            self.receive_port
        )
        logger.info(
            f"LogServer running on {self.host}:{self.send_port} for sending, " +
            f"and on {self.host}:{self.receive_port} for receiving"
        )

        try:
            await asyncio.gather(
                send_server.serve_forever(),
                receive_server.serve_forever()
            )
        except KeyboardInterrupt:
            pass

        send_server.close()
        receive_server.close()
        await asyncio.gather(
            send_server.wait_closed(),
            receive_server.wait_closed()
        )

    async def handle_connection(self, reader, writer):
        if self.number_of_connections <= MAX_NUMBER_OF_CONNECTIONS:
            self.number_of_connections += 1
            client_address = writer.get_extra_info('peername')
            logger.debug(f"Connection from {client_address} accepted")

            try:
                await self.send_logline(writer, self.get_next_logline())
            except asyncio.CancelledError:
                pass
            finally:
                logger.debug(f"Connection to {client_address} closed")
                writer.close()
                await writer.wait_closed()
                self.number_of_connections -= 1
        else:
            client_address = writer.get_extra_info('peername')
            logger.warning(
                f"Client connection to {client_address} denied. Max number of connections reached!"
            )  # TODO: Change to logging line
            writer.close()
            await writer.wait_closed()

    async def send_logline(self, writer, logline):
        if logline:
            writer.write(logline.encode('utf-8'))
            await writer.drain()
            logger.info(f"Logline sent: {logline}")
            return

        logger.info("No logline available")

    def get_next_logline(self) -> str | None:
        if not self.data_queue.empty():
            return self.data_queue.get()
        return None

    async def receive_logline(self, reader, writer):
        if self.number_of_connections <= MAX_NUMBER_OF_CONNECTIONS:
            self.number_of_connections += 1
            client_address = writer.get_extra_info('peername')
            logger.debug(f"Connection from {client_address} accepted")

            try:
                while True:
                    data = await reader.read(1024)
                    if not data:
                        break
                    received_message = data.decode()
                    logger.info(f"Received message: {received_message}")
                    self.data_queue.put(received_message)
            except asyncio.CancelledError:
                pass
            finally:
                logger.debug(f"Connection to {client_address} closed")
                writer.close()
                await writer.wait_closed()
                self.number_of_connections -= 1
        else:
            client_address = writer.get_extra_info('peername')
            logger.warning(
                f"Client connection to {client_address} denied. Max number of connections reached!"
            )  # TODO: Change to logging line
            writer.close()
            await writer.wait_closed()


server = LogServer("127.0.0.1", 9998, 9999)
asyncio.run(server.open())
