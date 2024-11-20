import asyncio
import os
import sys

import aiofiles

sys.path.append(os.getcwd())
from src.base.kafka_handler import (
    SimpleKafkaConsumeHandler,
    ExactlyOnceKafkaProduceHandler,
)
from src.base.utils import generate_unique_transactional_id
from src.base.utils import setup_config
from src.base.log_config import get_logger

module_name = "log_storage.logserver"
logger = get_logger(module_name)

config = setup_config()
CONSUME_TOPIC = config["environment"]["kafka_topics"]["pipeline"]["logserver_in"]
PRODUCE_TOPIC = config["environment"]["kafka_topics"]["pipeline"][
    "logserver_to_collector"
]
READ_FROM_FILE = config["pipeline"]["log_storage"]["logserver"]["input_file"]
KAFKA_BROKERS = ",".join(
    [
        f"{broker['hostname']}:{broker['port']}"
        for broker in config["environment"]["kafka_brokers"]
    ]
)


class LogServer:
    """
    Receives and sends single log lines. Listens for messages via Kafka and reads newly added lines from an input
    file.
    """

    def __init__(self) -> None:
        transactional_id = generate_unique_transactional_id(module_name, KAFKA_BROKERS)

        self.kafka_consume_handler = SimpleKafkaConsumeHandler(CONSUME_TOPIC)
        self.kafka_produce_handler = ExactlyOnceKafkaProduceHandler(transactional_id)

    async def start(self) -> None:
        """
        Starts fetching messages from Kafka and from the input file.
        """
        logger.info(
            "LogServer started:\n"
            f"    ⤷  receiving on Kafka topic '{CONSUME_TOPIC}'\n"
            f"    ⤷  receiving from input file '{READ_FROM_FILE}'\n"
            f"    ⤷  sending on Kafka topic '{PRODUCE_TOPIC}'"
        )

        try:
            await asyncio.gather(
                self.fetch_from_kafka(),
                self.fetch_from_file(),
            )
        except KeyboardInterrupt:
            logger.info("LogServer stopped.")

    def send(self, message: str) -> None:
        """
        Sends a received message using Kafka.

        Args:
            message (str): Message to be sent
        """
        self.kafka_produce_handler.produce(topic=PRODUCE_TOPIC, data=message)
        logger.debug(f"Sent: '{message}'")

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

            self.send(value)

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
                    self.send(cleaned_line)


def main() -> None:
    """
    Creates the :class:`LogServer` instance and starts it.
    """
    server_instance = LogServer()
    asyncio.run(server_instance.start())


if __name__ == "__main__":  # pragma: no cover
    main()
