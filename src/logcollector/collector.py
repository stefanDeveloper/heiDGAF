import asyncio
import datetime
import ipaddress
import json
import os
import sys
import uuid

sys.path.append(os.getcwd())
from src.base.clickhouse_kafka_sender import ClickHouseKafkaSender
from src.base.kafka_handler import ExactlyOnceKafkaConsumeHandler
from src.base.logline_handler import LoglineHandler
from src.base import utils
from src.logcollector.batch_handler import BufferedBatchSender
from src.base.log_config import get_logger

module_name = "log_collection.collector"
logger = get_logger(module_name)

config = utils.setup_config()
IPV4_PREFIX_LENGTH = config["pipeline"]["log_collection"]["batch_handler"]["subnet_id"][
    "ipv4_prefix_length"
]
IPV6_PREFIX_LENGTH = config["pipeline"]["log_collection"]["batch_handler"]["subnet_id"][
    "ipv6_prefix_length"
]
TIMESTAMP_FORMAT = config["environment"]["timestamp_format"]
REQUIRED_FIELDS = ["timestamp", "status_code", "client_ip", "record_type"]
BATCH_SIZE = config["pipeline"]["log_collection"]["batch_handler"]["batch_size"]
CONSUME_TOPIC = config["environment"]["kafka_topics"]["pipeline"][
    "logserver_to_collector"
]


class LogCollector:
    """
    Consumes incoming log lines from the :class:`LogServer`. Validates all data fields by type and
    value, invalid loglines are discarded. All valid loglines are sent to the Batch Sender.
    """

    def __init__(self) -> None:
        self.loglines = asyncio.Queue()
        self.batch_handler = BufferedBatchSender()
        self.logline_handler = LoglineHandler()
        self.kafka_consume_handler = ExactlyOnceKafkaConsumeHandler(CONSUME_TOPIC)

        # databases
        self.failed_dns_loglines = ClickHouseKafkaSender("failed_dns_loglines")
        self.dns_loglines = ClickHouseKafkaSender("dns_loglines")
        self.logline_timestamps = ClickHouseKafkaSender("logline_timestamps")

    async def start(self) -> None:
        """
        Starts fetching messages from Kafka and sending them to the :class:`Prefilter`.
        """
        logger.info(
            "LogCollector started:\n"
            f"    ⤷  receiving on Kafka topic '{CONSUME_TOPIC}'"
        )

        task_fetch = asyncio.Task(self.fetch())
        task_send = asyncio.Task(self.send())

        try:
            await asyncio.gather(
                task_fetch,
                task_send,
            )
        except KeyboardInterrupt:
            task_fetch.cancel()
            task_send.cancel()

            logger.info("LogCollector stopped.")

    async def fetch(self) -> None:
        """
        Starts a loop to continuously listen on the configured Kafka topic. If a message is consumed, it is
        decoded and stored.
        """
        loop = asyncio.get_running_loop()

        while True:
            key, value, topic = await loop.run_in_executor(
                None, self.kafka_consume_handler.consume
            )
            logger.debug(f"From Kafka: '{value}'")

            await self.store(datetime.datetime.now(), value)

    async def send(self) -> None:
        """
        Continuously sends the next logline in JSON format to the BatchSender, where it is stored in
        a temporary batch before being sent to the Prefilter. Adds a subnet ID to the message, that it retrieves
        from the client's IP address.
        """
        try:
            while True:
                if not self.loglines.empty():
                    timestamp_in, logline = await self.loglines.get()

                    try:
                        fields = self.logline_handler.validate_logline_and_get_fields_as_json(
                            logline
                        )
                    except ValueError:
                        self.failed_dns_loglines.insert(
                            dict(
                                message_text=logline,
                                timestamp_in=timestamp_in,
                                timestamp_failed=datetime.datetime.now(),
                                reason_for_failure=None,  # TODO: Add actual reason
                            )
                        )
                        continue

                    subnet_id = self.get_subnet_id(
                        ipaddress.ip_address(fields.get("client_ip"))
                    )

                    additional_fields = fields.copy()
                    for field in REQUIRED_FIELDS:
                        additional_fields.pop(field)

                    logline_id = uuid.uuid4()

                    self.dns_loglines.insert(
                        dict(
                            logline_id=logline_id,
                            subnet_id=subnet_id,
                            timestamp=datetime.datetime.strptime(
                                fields.get("timestamp"), TIMESTAMP_FORMAT
                            ),
                            status_code=fields.get("status_code"),
                            client_ip=fields.get("client_ip"),
                            record_type=fields.get("record_type"),
                            additional_fields=json.dumps(additional_fields),
                        )
                    )

                    self.logline_timestamps.insert(
                        dict(
                            logline_id=logline_id,
                            stage=module_name,
                            status="in_process",
                            timestamp=timestamp_in,
                            is_active=True,
                        )
                    )

                    message_fields = fields.copy()
                    message_fields["logline_id"] = str(logline_id)

                    self.batch_handler.add_message(
                        subnet_id, json.dumps(message_fields)
                    )

                    self.logline_timestamps.insert(
                        dict(
                            logline_id=logline_id,
                            stage=module_name,
                            status="finished",
                            timestamp=datetime.datetime.now(),
                            is_active=True,
                        )
                    )
                    logger.debug(f"Sent: '{logline}'")
                else:
                    await asyncio.sleep(0.1)
        except KeyboardInterrupt:
            while not self.loglines.empty():
                logline = await self.loglines.get()
                fields = self.logline_handler.validate_logline_and_get_fields_as_json(
                    logline
                )
                subnet_id = self.get_subnet_id(
                    ipaddress.ip_address(fields.get("client_ip"))
                )

                self.batch_handler.add_message(subnet_id, json.dumps(fields))

            logger.info("Stopped LogCollector.")

    async def store(self, timestamp_in: datetime.datetime, message: str):
        """
        Stores the given message temporarily.

        Args:
            timestamp_in (datetime.datetime): Timestamp of entering the pipeline
            message (str): Message to be stored
        """
        await self.loglines.put((timestamp_in, message))

    @staticmethod
    def get_subnet_id(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> str:
        """
        Returns the subnet ID of an IP address.

        Args:
            address (ipaddress.IPv4Address | ipaddress.IPv6Address): IP address to get the subnet ID for

        Returns:
            subnet ID for the given IP address as string
        """
        if isinstance(address, ipaddress.IPv4Address):
            normalized_ip_address, prefix_length = utils.normalize_ipv4_address(
                address, IPV4_PREFIX_LENGTH
            )
        elif isinstance(address, ipaddress.IPv6Address):
            normalized_ip_address, prefix_length = utils.normalize_ipv6_address(
                address, IPV6_PREFIX_LENGTH
            )
        else:
            raise ValueError("Unsupported IP address type")

        return f"{normalized_ip_address}_{prefix_length}"


def main() -> None:
    """
    Creates the :class:`LogCollector` instance and starts it.
    """
    collector_instance = LogCollector()
    asyncio.run(collector_instance.start())


if __name__ == "__main__":  # pragma: no cover
    main()
