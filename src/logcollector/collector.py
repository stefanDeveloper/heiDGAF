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
from collections import defaultdict

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
    "ts",
    "status_code",
    "client_ip",
    "record_type",
]
BATCH_SIZE = config["pipeline"]["log_collection"]["batch_handler"]["batch_size"]

PRODUCE_TOPIC_PREFIX = config["environment"]["kafka_topics_prefix"]["pipeline"]["batch_sender_to_prefilter"]
CONSUME_TOPIC_PREFIX = config["environment"]["kafka_topics_prefix"]["pipeline"]["logserver_to_collector"]

PROTOCOLS_TO_SENSOR_TOPICS = defaultdict(list)
for sensor in config["pipeline"]["zeek"]["sensors"].values():
    for mapping in sensor.get("protocol_to_topic", []):
        for protocol, topics in mapping.items():
            for topic in topics:
                PROTOCOLS_TO_SENSOR_TOPICS[protocol].append(topic)
for k,v in PROTOCOLS_TO_SENSOR_TOPICS.items():
    PROTOCOLS_TO_SENSOR_TOPICS[k] = set(PROTOCOLS_TO_SENSOR_TOPICS[k])
class LogCollector:
    """Consumes incoming log lines from the :class:`LogServer`. Validates all data fields by type and
    value, invalid loglines are discarded. All valid loglines are sent to the batch sender.
    """
    def __init__(self, protocol, consume_topic, produce_topic) -> None:
        self.protocol = protocol
        self.consume_topic = consume_topic
        self.loglines = asyncio.Queue()
        self.batch_handler = BufferedBatchSender(produce_topic=produce_topic)
        self.logline_handler = LoglineHandler(protocol)
        self.kafka_consume_handler = ExactlyOnceKafkaConsumeHandler(consume_topic)

        # databases
        self.failed_dns_loglines = ClickHouseKafkaSender(f"failed_dns_loglines")
        self.dns_loglines = ClickHouseKafkaSender(f"dns_loglines")
        self.logline_timestamps = ClickHouseKafkaSender("logline_timestamps")
        self.fill_levels = ClickHouseKafkaSender("fill_levels")

        self.fill_levels.insert(
            dict(
                timestamp=datetime.datetime.now(),
                stage=module_name,
                entry_type="total_loglines",
                entry_count=0,
            )
        )

    async def start(self) -> None:
        """Starts fetching messages from Kafka and sending them to the :class:`Prefilter`."""
        logger.info(
            "LogCollector started:\n"
            f"    ⤷  receiving on Kafka topic '{self.consume_topic}'"
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
        """Starts a loop to continuously listen on the configured Kafka topic. If a message is consumed, it is
        decoded and sent."""
        import time
        loop = asyncio.get_running_loop()

        while True:
            key, value, topic = await loop.run_in_executor(
                None, self.kafka_consume_handler.consume
            )
            logger.debug(f"From Kafka: '{value}'")

            self.send(datetime.datetime.now(), value)

    def send(self, timestamp_in: datetime.datetime, message: str) -> None:
        """Sends the logline in JSON format to the BatchSender, where it is stored in
        a temporary batch before being sent to the :class:`Prefilter`. Adds the subnet ID to the message.

        Args:
            timestamp_in (datetime.datetime): Timestamp of entering the pipeline
            message (str): Message to be stored
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
        # TODO make the code less hardcoded in variables!       
        additional_fields = fields.copy()
        for field in REQUIRED_FIELDS:
            additional_fields.pop(field)

        subnet_id = self._get_subnet_id(ipaddress.ip_address(fields.get("client_ip")))
        logline_id = uuid.uuid4()

        # TODO required types per protocol
        self.dns_loglines.insert(
            dict(
                logline_id=logline_id,
                subnet_id=subnet_id,
                timestamp=datetime.datetime.fromisoformat(fields.get("ts")),
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
        logger.info(f"Sent: {message}")

    @staticmethod
    def _get_subnet_id(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> str:
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


async def main() -> None:
    """
    Creates the :class:`LogCollector` instance and starts it.
    """
    tasks = []
    for protocol,topics in PROTOCOLS_TO_SENSOR_TOPICS.items():
        for topic in topics:
            consume_topic = f"{CONSUME_TOPIC_PREFIX}-{topic}"
            produce_topic = f"{PRODUCE_TOPIC_PREFIX}-{topic}"
            collector_instance = LogCollector(protocol=protocol,consume_topic=consume_topic, produce_topic=produce_topic)
            tasks.append(
                asyncio.create_task(collector_instance.start())
            )

    await asyncio.gather(*tasks)
if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
