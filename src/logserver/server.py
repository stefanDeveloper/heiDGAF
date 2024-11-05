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
    """
    Server for receiving, storing and sending single log lines. Opens a port for receiving messages, listens for
    messages via Kafka and reads newly added lines from an input file. To retrieve a message from the server,
    other modules can connect to its outgoing/sending port. The server will then send its oldest message as a response.
    """

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

    async def open(self) -> None:
        """
        Opens both ports for sending and receiving and starts reading from the input file as well as listening for
        messages via Kafka. Can be stopped via a ``KeyboardInterrupt``.
        """
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
        finally:
            send_server.close()
            receive_server.close()
            await asyncio.gather(
                send_server.wait_closed(), receive_server.wait_closed()
            )
            logger.debug("Both sockets closed.")

    async def handle_connection(self, reader, writer, sending: bool) -> None:
        """
        Handles new incoming connection attempts. If the maximum number of possible connections is not yet reached, the
        connection is approved and the log line is sent or received, depending on the calling method. If the number is
        reached, a warning message will be printed and no connection gets established.

        Args:
            reader: Responsible for reading incoming data
            writer: Responsible for writing outgoing data
            sending (bool): Sending if True, receiving otherwise
        """
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

    async def handle_kafka_inputs(self) -> None:
        """
        Starts a loop to continuously listen on the configured Kafka topic. If a message is consumed, it is added
        to the data queue.
        """
        loop = asyncio.get_running_loop()

        while True:
            key, value = await loop.run_in_executor(
                None, self.kafka_consume_handler.consume
            )
            logger.info(f"Received message via Kafka:\n    ⤷  {value}")
            self.data_queue.put(value)

    async def async_follow(self, file: str = READ_FROM_FILE) -> None:
        """
        Continuously checks for new lines at the end of the input file. If one or multiple new lines are found, any
        empty lines are removed and the remaining lines added to the data queue.

        Args:
            file (str): File to be read as string
        """
        async with aiofiles.open(file, mode="r") as file:
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

    async def handle_send_logline(self, reader, writer) -> None:
        """
        Handles the sending of a logline by calling :meth:`handle_connection` with ``sending=True``.

        Args:
            reader: Responsible for reading incoming data
            writer: Responsible for writing outgoing data
        """
        logger.debug("Calling handle_connection with sending=True...")
        await self.handle_connection(reader, writer, True)

    async def handle_receive_logline(self, reader, writer) -> None:
        """
        Handles the receiving of a logline by calling :meth:`handle_connection` with ``sending=False``.

        Args:
            reader: Responsible for reading incoming data
            writer: Responsible for writing outgoing data
        """
        logger.debug("Calling handle_connection with sending=False...")
        await self.handle_connection(reader, writer, False)

    @staticmethod
    async def send_logline(writer, logline) -> None:
        """
        Sends the given log line encoded as UTF-8 to the connected component.

        Args:
            writer: Responsible for writing outgoing data
            logline: Logline to be sent
        """
        if logline:
            logger.debug(f"Sending {logline=}...")
            writer.write(logline.encode("utf-8"))
            await writer.drain()
            logger.info(f"Sent message:\n    ⤷  {logline}")
            return

        logger.debug("No logline available")

    async def receive_logline(self, reader) -> None:
        """
        Receives one or multiple log lines encoded as UTF-8 separated by and ending with separator '\n' from the
        connected component and adds it or them to the data queue. Message must end with separator symbol.

        Args:
            reader: Responsible for reading incoming data
        """
        while True:
            try:
                data = await reader.readuntil(separator=b"\n")
                if not data:
                    break
                received_message = data.decode().strip()
                logger.info(f"Received message:\n    ⤷  {received_message}")
                self.data_queue.put(received_message)
            except asyncio.exceptions.IncompleteReadError as e:
                logger.warning(f"Ignoring message: No separator symbol found: {e}")
                break
            except asyncio.LimitOverrunError:
                logger.error(f"Message size exceeded, separator symbol not found")
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                raise

    def get_next_logline(self) -> str | None:
        """
        Returns and removes the oldest log line in the data queue.

        Returns:
            Oldest log line in the data queue.
        """
        logger.debug("Getting next available logline...")
        if not self.data_queue.empty():
            logger.debug("Returning logline...")
            return self.data_queue.get()
        return None


def main() -> None:
    """
    Creates the :class:`LogServer` instance and starts it.
    """
    logger.info("Starting LogServer...")
    server_instance = LogServer()
    logger.debug("LogServer started. Opening sockets...")

    asyncio.run(server_instance.open())


if __name__ == "__main__":  # pragma: no cover
    main()
