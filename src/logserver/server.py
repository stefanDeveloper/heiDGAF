import asyncio
import os
import queue
import sys

import aiofiles

sys.path.append(os.getcwd())
from src.base.kafka_handler import KafkaConsumeHandler
from src.base.utils import setup_config
from src.base import utils
from src.base.log_config import get_logger

logger = get_logger("log_storage.logserver")

CONFIG = setup_config()
HOSTNAME = CONFIG["environment"]["logserver"]["hostname"]
PORT_IN = CONFIG["environment"]["logserver"]["port_in"]
PORT_OUT = CONFIG["environment"]["logserver"]["port_out"]
MAX_NUMBER_OF_CONNECTIONS = CONFIG["pipeline"]["log_storage"]["logserver"][
    "max_number_of_connections"
]
LISTEN_ON_TOPIC = CONFIG["pipeline"]["log_storage"]["logserver"]["input_kafka_topic"]
READ_FROM_FILE = CONFIG["pipeline"]["log_storage"]["logserver"]["input_file"]


class LogServer:
    def __init__(self) -> None:
        logger.debug("Initializing LogServer...")
        self.host = None
        self.port_out = None
        self.port_in = None

        logger.debug("Validating host name...")
        self.host = utils.validate_host(HOSTNAME)
        logger.debug("Host name is valid. Validating ingoing port...")
        self.port_in = utils.validate_port(PORT_IN)
        logger.debug("Ingoing port is valid. Validating outgoing port...")
        self.port_out = utils.validate_port(PORT_OUT)
        logger.debug("Outgoing port is valid.")
        logger.debug("Initialized LogServer.")

        self.socket = None
        self.number_of_connections = 0
        self.data_queue = queue.Queue()
        self.kafka_consume_handler = KafkaConsumeHandler(topic=LISTEN_ON_TOPIC)

    async def open(self):
        logger.debug("Opening LogServer sockets...")
        logger.debug(f"Creating the sending socket on port {self.port_out}...")
        send_server = await asyncio.start_server(
            self.handle_send_logline, str(self.host), self.port_out
        )
        logger.debug(f"Creating the receiving socket on port {self.port_in}...")
        receive_server = await asyncio.start_server(
            self.handle_receive_logline, str(self.host), self.port_in
        )
        logger.info(
            "LogServer is running:\n"
            f"    ⤷  receiving on {self.host}:{self.port_in} and Kafka topic '{LISTEN_ON_TOPIC}'\n"
            f"    ⤷  sending on {self.host}:{self.port_out}"
        )

        try:
            await asyncio.gather(
                send_server.serve_forever(),
                receive_server.serve_forever(),
                self.handle_kafka_inputs(),
                self.async_follow(),
            )
        except KeyboardInterrupt:
            logger.debug("Stop serving...")

        send_server.close()
        receive_server.close()
        await asyncio.gather(send_server.wait_closed(), receive_server.wait_closed())
        logger.debug("Both sockets closed.")

    async def handle_connection(self, reader, writer, sending: bool):
        logger.debug(f"Handling connection with {sending=}...")
        if self.number_of_connections < MAX_NUMBER_OF_CONNECTIONS:
            logger.debug(
                f"Adding connection to {self.number_of_connections}/{MAX_NUMBER_OF_CONNECTIONS}) open "
                f"connections..."
            )
            self.number_of_connections += 1
            client_address = writer.get_extra_info("peername")
            logger.debug(f"Connection from {client_address} accepted")

            try:
                if sending:
                    logger.debug(
                        "Sending active: Calling send_logline for next available logline..."
                    )
                    await self.send_logline(writer, self.get_next_logline())
                else:
                    logger.debug("Receiving: Calling receive_logline...")
                    await self.receive_logline(reader)
            except asyncio.CancelledError:
                logger.debug("Handling cancelled.")
                pass
            finally:
                writer.close()
                await writer.wait_closed()
                self.number_of_connections -= 1
                logger.debug(f"Connection to {client_address} closed.")
        else:
            client_address = writer.get_extra_info("peername")
            logger.warning(
                f"Client connection to {client_address} denied. Max number of connections reached!"
            )
            writer.close()
            await writer.wait_closed()

    async def handle_kafka_inputs(self):
        loop = asyncio.get_running_loop()

        while True:
            key, value = await loop.run_in_executor(
                None, self.kafka_consume_handler.consume
            )
            logger.info(f"Received message via Kafka:\n    ⤷  {value}")
            self.data_queue.put(value)

    async def async_follow(self, file: str = READ_FROM_FILE):
        async with aiofiles.open(file, mode='r') as file:
            # jump to end of file
            await file.seek(0, 2)

            while True:
                lines = await file.readlines()
                if not lines:
                    await asyncio.sleep(0.1)
                    continue

                for line in lines:
                    # remove empty lines
                    cleaned_line = line.strip()
                    if not cleaned_line:
                        continue

                    logger.info(f"Extracted message from file:\n    ⤷  {cleaned_line}")
                    self.data_queue.put(cleaned_line)

    async def handle_send_logline(self, reader, writer):
        logger.debug("Calling handle_connection with sending=True...")
        await self.handle_connection(reader, writer, True)

    async def handle_receive_logline(self, reader, writer):
        logger.debug("Calling handle_connection with sending=False...")
        await self.handle_connection(reader, writer, False)

    @staticmethod
    async def send_logline(writer, logline):
        if logline:
            logger.debug(f"Sending {logline=}...")
            writer.write(logline.encode("utf-8"))
            await writer.drain()
            logger.info(f"Sent message:\n    ⤷  {logline}")
            return

        logger.debug("No logline available")

    async def receive_logline(self, reader):
        while True:
            data = await reader.read(1024)
            if not data:
                break
            received_message = data.decode()
            logger.info(f"Received message:\n    ⤷  {received_message}")
            self.data_queue.put(received_message)

    def get_next_logline(self) -> str | None:
        logger.debug("Getting next available logline...")
        if not self.data_queue.empty():
            logger.debug("Returning logline...")
            return self.data_queue.get()
        return None

    # TODO: Add a close method


def main():
    logger.info("Starting LogServer...")
    server_instance = LogServer()
    logger.debug("LogServer started. Opening sockets...")

    asyncio.run(server_instance.open())


if __name__ == "__main__":  # pragma: no cover
    main()
