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

REQUIRED_FIELDS = [
    "ts",
    "src_ip",
]
PRODUCE_TOPIC_PREFIX = config["environment"]["kafka_topics_prefix"]["pipeline"][
    "batch_sender_to_prefilter"
]
CONSUME_TOPIC_PREFIX = config["environment"]["kafka_topics_prefix"]["pipeline"][
    "logserver_to_collector"
]

SENSOR_PROTOCOLS = utils.get_zeek_sensor_topic_base_names(config)
PREFILTERS = config["pipeline"]["log_filtering"]

COLLECTORS = [
    collector for collector in config["pipeline"]["log_collection"]["collectors"]
]


class LogCollector:
    """Consumes incoming log lines from the :class:`LogServer`. Validates all data fields by type and
    value, invalid loglines are discarded. All valid loglines are sent to the batch sender.
    """

    def __init__(
        self, collector_name, protocol, consume_topic, produce_topics, validation_config
    ) -> None:
        """Initializes a new LogCollector instance with the specified configuration.

        Args:
            collector_name (str): Name of the collector instance
            protocol (str): Protocol type of the log lines (e.g., 'dns', 'http')
            consume_topic (str): Kafka topic to consume log lines from
            produce_topics (list[str]): List of Kafka topics to produce validated log lines to
            validation_config (list): Configuration for validating log line fields
        """
        self.protocol = protocol
        self.consume_topic = consume_topic
        self.batch_configuration = utils.get_batch_configuration(collector_name)
        self.loglines = asyncio.Queue()
        self.batch_handler = BufferedBatchSender(
            produce_topics=produce_topics, collector_name=collector_name
        )
        self.logline_handler = LoglineHandler(validation_config)
        self.kafka_consume_handler = ExactlyOnceKafkaConsumeHandler(consume_topic)

        # databases
        self.failed_protocol_loglines = ClickHouseKafkaSender("failed_loglines")
        self.protocol_loglines = ClickHouseKafkaSender("loglines")
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
        """Starts the LogCollector processing loop.

        This method initializes the Kafka message fetching process and runs it in an executor
        to avoid blocking the asyncio event loop. It logs the startup information and
        continues processing until interrupted.

        """
        logger.info(
            "LogCollector started:\n"
            f"    ⤷  receiving on Kafka topic '{self.consume_topic}'"
        )
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.fetch)

        logger.info("LogCollector stopped.")

    def fetch(self) -> None:
        """Continuously listens for messages on the configured Kafka topic.

        This method runs in an infinite loop, consuming messages from Kafka and
        processing them through the send method. It blocks until messages are
        available on the Kafka topic.

        Note:
            This method is intended to be run in a separate thread via run_in_executor
            since it contains a blocking loop.
        """

        while True:
            key, value, topic = self.kafka_consume_handler.consume()
            logger.debug(f"From Kafka: '{value}'")
            self.send(datetime.datetime.now(), value)

    def send(self, timestamp_in: datetime.datetime, message: str) -> None:
        """Processes and sends a log line to the batch handler after validation.

        This method:
        1. Validates the log line format and required fields
        2. Stores valid log lines in the database
        3. Calculates the subnet ID for batch processing
        4. Adds the log line to the batch handler

        Args:
            timestamp_in (datetime.datetime): Timestamp when the log line entered the pipeline
            message (str): Raw log line message in JSON format
        """
        try:
            fields = self.logline_handler.validate_logline_and_get_fields_as_json(
                message
            )
        except ValueError:
            self.failed_protocol_loglines.insert(
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
        subnet_id = self._get_subnet_id(ipaddress.ip_address(fields.get("src_ip")))
        logline_id = uuid.uuid4()
        self.protocol_loglines.insert(
            dict(
                logline_id=logline_id,
                subnet_id=subnet_id,
                timestamp=datetime.datetime.fromisoformat(fields.get("ts")),
                src_ip=fields.get("src_ip"),
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
        logger.debug(f"Sent: {message}")

    def _get_subnet_id(
        self, address: ipaddress.IPv4Address | ipaddress.IPv6Address
    ) -> str:
        """Calculates the subnet ID for an IP address based on batch configuration.

        This method normalizes the IP address to the configured subnet prefix length
        and returns a string representation of the subnet.

        Args:
            address (ipaddress.IPv4Address | ipaddress.IPv6Address): IP address to process

        Returns:
            str: Subnet ID in the format "network_address/prefix_length"
                Example: "192.168.1.0_24" or "2001:db8::/64"

        Raises:
            ValueError: If the address is neither IPv4 nor IPv6 address type

        """
        if isinstance(address, ipaddress.IPv4Address):
            normalized_ip_address, prefix_length = utils.normalize_ipv4_address(
                address, self.batch_configuration["subnet_id"]["ipv4_prefix_length"]
            )
        elif isinstance(address, ipaddress.IPv6Address):
            normalized_ip_address, prefix_length = utils.normalize_ipv6_address(
                address, self.batch_configuration["subnet_id"]["ipv6_prefix_length"]
            )
        else:
            raise ValueError("Unsupported IP address type")

        return f"{normalized_ip_address}_{prefix_length}"


async def main() -> None:
    """Creates and starts all configured LogCollector instances.

    This function:
    1. Iterates through all collectors defined in the configuration
    2. Creates a LogCollector instance for each collector
    3. Starts each collector in its own asyncio task
    4. Waits for all collectors to complete (which is effectively forever)

    """
    tasks = []

    for collector in COLLECTORS:
        protocol = collector["protocol_base"]
        consume_topic = f"{CONSUME_TOPIC_PREFIX}-{collector['name']}"
        produce_topics = [
            f"{PRODUCE_TOPIC_PREFIX}-{prefilter['name']}"
            for prefilter in PREFILTERS
            if collector["name"] == prefilter["collector_name"]
        ]
        validation_config = collector["required_log_information"]
        collector_instance = LogCollector(
            collector_name=collector["name"],
            protocol=protocol,
            consume_topic=consume_topic,
            produce_topics=produce_topics,
            validation_config=validation_config,
        )
        tasks.append(asyncio.create_task(collector_instance.start()))
    await asyncio.gather(*tasks)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
