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
REQUIRED_FIELDS = [
    "timestamp",
    "status_code",
    "client_ip",
    "record_type",
]
BATCH_SIZE = config["pipeline"]["log_collection"]["batch_handler"]["batch_size"]
CONSUME_TOPIC = config["environment"]["kafka_topics"]["pipeline"][
    "logserver_to_collector"
]


class LogCollector:
    """Main component of the Log Collection stage to pre-process and format data

    Consumes incoming loglines from the LogServer. Validates all data fields by type and
    value, invalid loglines are discarded. All valid loglines are sent to the BatchSender.
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
        """Starts the task to fetch data from Kafka."""
        logger.info(
            "LogCollector started:\n"
            f"    ⤷  receiving on Kafka topic '{CONSUME_TOPIC}'"
        )

        task_fetch = asyncio.Task(self.fetch())

        try:
            await asyncio.gather(
                task_fetch,
            )
        except KeyboardInterrupt:
            task_fetch.cancel()

            logger.info("LogCollector stopped.")

    async def fetch(self) -> None:
        """Fetches data from the configured Kafka topic in a loop.

        Starts an asynchronous loop to continuously listen on the configured Kafka topic and fetch new messages.
        If a message is consumed, it is decoded and sent.
        """
        loop = asyncio.get_running_loop()

        while True:
            key, value, topic = await loop.run_in_executor(
                None, self.kafka_consume_handler.consume
            )
            logger.debug(f"From Kafka: '{value}'")

            self.send(datetime.datetime.now(), value)

    def send(self, timestamp_in: datetime.datetime, message: str) -> None:
        """Adds a message to the BatchSender to be stored temporarily.

        The message is added in JSON format to the BatchSender, where it is stored in
        a temporary batch before being sent to the Prefilter. The subnet ID is added to the message.
        In the case that a message does not have a valid logline format, it is logged as a failed logline
        including timestamps of entering and being detected as invalid. In the case of a valid message, the logline's
        fields as well as an "in_process" event are logged using the timestamp of it entering the module. After
        processing, a "finished" event is logged for it.

        Args:
            timestamp_in (datetime.datetime): Timestamp of entering the pipeline.
            message (str): Message to be stored.
        """

        try:
            fields = self.logline_handler.validate_logline_and_get_fields_as_json(
                message
            )
        except ValueError:
            self.failed_dns_loglines.insert(
                dict(
                    message_text=message,
                    timestamp_in=timestamp_in,
                    timestamp_failed=datetime.datetime.now(),
                    reason_for_failure=None,  # TODO: Add actual reason
                )
            )
            return

        additional_fields = fields.copy()
        for field in REQUIRED_FIELDS:
            additional_fields.pop(field)

        subnet_id = self._get_subnet_id(ipaddress.ip_address(fields.get("client_ip")))
        logline_id = uuid.uuid4()

        self.dns_loglines.insert(
            dict(
                logline_id=logline_id,
                subnet_id=subnet_id,
                timestamp=datetime.datetime.fromisoformat(fields.get("timestamp")),
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

        self.logline_timestamps.insert(
            dict(
                logline_id=logline_id,
                stage=module_name,
                status="finished",
                timestamp=datetime.datetime.now(),
                is_active=True,
            )
        )

        self.batch_handler.add_message(subnet_id, json.dumps(message_fields))
        logger.debug(f"Sent: '{message}'")

    @staticmethod
    def _get_subnet_id(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> str:
        """Returns the subnet ID of an IP address.

        The subnet ID is formatted as `[NORMALIZED_IP_ADDRESS]_[PREFIX_LENGTH]`.
        Depending on the IP address, the configuration value
        ``pipeline.log_collection.batch_handler.subnet_id.[ipv4_prefix_length | ipv6_prefix_length]``
        is used as `PREFIX_LENGTH`.

        For example, the IPv4 address `192.168.1.1` with prefix length `24` is formatted to ``192.168.1.0_24``.

        Args:
            address (ipaddress.IPv4Address | ipaddress.IPv6Address): IP address to get the subnet ID for.

        Returns:
            Subnet ID for the given IP address as string.
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
    """Creates the :class:`LogCollector` instance and starts it."""
    collector_instance = LogCollector()
    asyncio.run(collector_instance.start())


if __name__ == "__main__":  # pragma: no cover
    main()
