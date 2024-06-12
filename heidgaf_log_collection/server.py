import asyncio
import logging
import os  # needed for Terminal execution
import queue
import sys  # needed for Terminal execution

sys.path.append(os.getcwd())  # needed for Terminal execution
from heidgaf_core import utils
from heidgaf_core.logging import setup_logging
from heidgaf_core.config import *

setup_logging()
logger = logging.getLogger(__name__)


class LogServer:
    def __init__(self, host: str, port_in: int, port_out: int) -> None:
        self.host = None
        self.port_out = None
        self.port_in = None
        self.socket = None
        self.number_of_connections = 0

        self.host = utils.validate_host(host)
        self.port_in = utils.validate_port(port_in)
        self.port_out = utils.validate_port(port_out)
        self.data_queue = queue.Queue()

    async def open(self):
        send_server = await asyncio.start_server(
            self.handle_send_logline,
            str(self.host),
            self.port_out
        )
        receive_server = await asyncio.start_server(
            self.handle_receive_logline,
            str(self.host),
            self.port_in
        )
        logger.info(
            f"LogServer running on {self.host}:{self.port_out} for sending, " +
            f"and on {self.host}:{self.port_in} for receiving"
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

    async def handle_connection(self, reader, writer, sending: bool):
        if self.number_of_connections <= MAX_NUMBER_OF_CONNECTIONS:
            self.number_of_connections += 1
            client_address = writer.get_extra_info('peername')
            logger.debug(f"Connection from {client_address} accepted")

            try:
                if sending:
                    await self.send_logline(writer, self.get_next_logline())
                else:
                    await self.receive_logline(reader)
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
            )
            writer.close()
            await writer.wait_closed()

    async def handle_send_logline(self, reader, writer):
        await self.handle_connection(reader, writer, True)

    async def handle_receive_logline(self, reader, writer):
        await self.handle_connection(reader, writer, False)

    @staticmethod
    async def send_logline(writer, logline):
        if logline:
            writer.write(logline.encode('utf-8'))
            await writer.drain()
            logger.info(f"Logline sent: {logline}")
            return

        logger.debug("No logline available")

    async def receive_logline(self, reader):
        while True:
            data = await reader.read(1024)
            if not data:
                break
            received_message = data.decode()
            logger.info(f"Received message: {received_message}")
            self.data_queue.put(received_message)

    def get_next_logline(self) -> str | None:
        if not self.data_queue.empty():
            return self.data_queue.get()
        return None


if __name__ == '__main__':
    server = LogServer("127.0.0.1", 9999, 9998)
    asyncio.run(server.open())
