import asyncio
import os
import sys
from asyncio import Lock

import aiofiles

sys.path.append(os.getcwd())
from src.base.kafka_handler import SimpleKafkaConsumeHandler, SimpleKafkaProduceHandler
from src.base.utils import setup_config
from src.base import utils
from src.base.log_config import get_logger

logger = get_logger("log_storage.logserver")

CONFIG = setup_config()
HOSTNAME = CONFIG["environment"]["logserver"]["hostname"]
LISTEN_ON_TOPIC = CONFIG["pipeline"]["log_storage"]["logserver"]["input_kafka_topic"]
SEND_TO_TOPIC = "logserver_to_collector"  # TODO: Change
READ_FROM_FILE = CONFIG["pipeline"]["log_storage"]["logserver"]["input_file"]


class LogServer:
    """
    Receives and sends single log lines. Listens for messages via Kafka and reads newly added lines from an input
    file.
    """

    def __init__(self) -> None:
        self.host = None
        self.host = utils.validate_host(HOSTNAME)

        self.lock = Lock()

        self.kafka_consume_handler = SimpleKafkaConsumeHandler(topics=LISTEN_ON_TOPIC)
        self.kafka_produce_handler = SimpleKafkaProduceHandler(
            transactional_id="TODO: Change"
        )

    async def start(self) -> None:
        """
        Starts fetching messages from Kafka and from the input file.
        """
        logger.info(
            "LogServer started:\n"
            f"    ⤷  receiving on Kafka topic '{LISTEN_ON_TOPIC}'\n"
            f"    ⤷  receiving from input file '{READ_FROM_FILE}'\n"
            f"    ⤷  sending on Kafka topic 'TODO'"
        )

        try:
            await asyncio.gather(
                self.fetch_from_kafka(),
                self.fetch_from_file(),
            )
        except KeyboardInterrupt:
            logger.info("LogServer stopped.")

    async def send(self, message: str) -> None:
        """
        Sends a received message using Kafka.

        Args:
            message (str): Message to be sent
        """
        async with self.lock:
            self.kafka_produce_handler.produce(topic=SEND_TO_TOPIC, data=message)

    async def fetch_from_kafka(self) -> None:
        """
        Starts a loop to continuously listen on the configured Kafka topic. If a message is consumed, it is sent.
        """
        loop = asyncio.get_running_loop()

        while True:
            key, value, topic = await loop.run_in_executor(
                None, self.kafka_consume_handler.consume
            )

            logger.debug(f"From Kafka: '{value}'")
            await self.send(value)

    async def fetch_from_file(self, file: str = READ_FROM_FILE) -> None:
        """
        Continuously checks for new lines at the end of the input file. If one or multiple new lines are found, any
        empty lines are removed and the remaining lines are sent individually.

        Args:
            file (str): Filename of the file to be read
        """
        async with aiofiles.open(file, mode="r") as file:
            await file.seek(0, 2)  # jump to end of file

            while True:
                lines = await file.readlines()

                if not lines:
                    await asyncio.sleep(0.1)
                    continue

                for line in lines:
                    cleaned_line = line.strip()  # remove empty lines

                    if not cleaned_line:
                        continue

                    logger.debug(f"From file: '{cleaned_line}'")
                    await self.send(cleaned_line)


def main() -> None:
    """
    Creates the :class:`LogServer` instance and starts it.
    """
    server_instance = LogServer()
    asyncio.run(server_instance.start())


if __name__ == "__main__":  # pragma: no cover
    main()
