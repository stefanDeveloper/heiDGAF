import datetime
import os
import sys
import uuid
import asyncio
import marshmallow_dataclass
from collections import defaultdict

sys.path.append(os.getcwd())
from src.base.clickhouse_kafka_sender import ClickHouseKafkaSender
from src.base.data_classes.batch import Batch
from src.base.logline_handler import LoglineHandler
from src.base.kafka_handler import (
    ExactlyOnceKafkaProduceHandler,
    ExactlyOnceKafkaConsumeHandler,
    KafkaMessageFetchException,
)
from src.base.log_config import get_logger
from src.base.utils import (
    setup_config,
    get_zeek_sensor_topic_base_names,
    generate_collisions_resistant_uuid,
)

module_name = "log_filtering.prefilter"
logger = get_logger(module_name)

config = setup_config()

CONSUME_TOPIC_PREFIX = config["environment"]["kafka_topics_prefix"]["pipeline"][
    "batch_sender_to_prefilter"
]
PRODUCE_TOPIC_PREFIX = config["environment"]["kafka_topics_prefix"]["pipeline"][
    "prefilter_to_inspector"
]

SENSOR_PROTOCOLS = get_zeek_sensor_topic_base_names(config)
PREFILTERS = config["pipeline"]["log_filtering"]
INSPECTORS = config["pipeline"]["data_inspection"]
COLLECTORS = [
    collector for collector in config["pipeline"]["log_collection"]["collectors"]
]
KAFKA_BROKERS = ",".join(
    [
        f"{broker['hostname']}:{broker['port']}"
        for broker in config["environment"]["kafka_brokers"]
    ]
)


class Prefilter:
    """
    Loads the data from the topic ``Prefilter`` and filters it so that only entries with the given status type(s) are
    kept. Filtered data is then sent using topic ``Inspect``.
    """

    def __init__(
        self, validation_config, consume_topic, produce_topics, relevance_function_name
    ):
        """Initializes a new ``Prefilter`` instance with the specified configuration.

        Args:
            validation_config (list): Configuration for validating log line fields
            consume_topic (str): Kafka topic to consume data from
            produce_topics (list[str]): Kafka topics to produce filtered data to
            relevance_function_name (str): Name of the relevance function to apply
            
        """
        self.name = None
        self.consume_topic = consume_topic
        self.produce_topics = produce_topics
        self.batch_id = None
        self.begin_timestamp = None
        self.end_timestamp = None
        self.subnet_id = None
        self.parent_row_id = None
        self.relevance_function_name = relevance_function_name

        self.unfiltered_data = []
        self.filtered_data = []

        self.logline_handler = LoglineHandler(validation_config)
        self.kafka_consume_handler = ExactlyOnceKafkaConsumeHandler(self.consume_topic)
        self.kafka_produce_handler = ExactlyOnceKafkaProduceHandler()

        # databases
        self.batch_tree = ClickHouseKafkaSender("batch_tree")
        self.batch_timestamps = ClickHouseKafkaSender("batch_timestamps")
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

    def get_and_fill_data(self) -> None:
        """Retrieves and processes new data from Kafka.

        This method:
        1. Clears any existing data
        2. Consumes a new batch of data from Kafka
        3. Extracts batch metadata (ID, timestamps, subnet ID)
        4. Stores the unfiltered data internally
        5. Records processing timestamps and metrics

        Note:
            This method blocks until data is available on the Kafka topic.
            Empty batches are handled gracefully but logged for monitoring.
        """
        
        self.clear_data()  # clear in case we already have data stored
        key, data = self.kafka_consume_handler.consume_as_object()
        self.subnet_id = key
        if data.data:
            self.parent_row_id = data.batch_tree_row_id
            self.batch_id = data.batch_id
            self.begin_timestamp = data.begin_timestamp
            self.end_timestamp = data.end_timestamp
            self.unfiltered_data = data.data
        self.batch_timestamps.insert(
            dict(
                batch_id=self.batch_id,
                stage=module_name,
                instance_name=self.name,
                status="in_process",
                timestamp=datetime.datetime.now(),
                is_active=True,
                message_count=len(self.unfiltered_data),
            )
        )

        row_id = generate_collisions_resistant_uuid()

        self.batch_tree.insert(
            dict(
                batch_row_id=row_id,
                stage=module_name,
                instance_name=self.name,
                status="in_process",
                timestamp=datetime.datetime.now(),
                parent_batch_row_id=self.parent_row_id,
                batch_id=self.batch_id,
            )
        )

        self.fill_levels.insert(
            dict(
                timestamp=datetime.datetime.now(),
                stage=module_name,
                entry_type="total_loglines",
                entry_count=len(self.unfiltered_data),
            )
        )

        if not self.unfiltered_data:
            logger.info(
                f"Received message:\n"
                f"    ⤷  Empty data field: No unfiltered data available. subnet_id: '{self.subnet_id}'"
            )
        else:
            logger.info(
                f"{self.consume_topic} Received message:\n"
                f"    ⤷  Contains data field of {len(self.unfiltered_data)} message(s) with "
                f"subnet_id: '{self.subnet_id}'."
            )

    def check_data_relevance_using_rules(self) -> None:
        """Applies relevance filtering to the unfiltered data.

        This method:
        1. Iterates through each log line in unfiltered_data
        2. Applies the configured relevance function to determine if the log line is relevant
        3. Adds relevant log lines to filtered_data
        4. Records non-relevant log lines as filtered out in the database

        Note:
            The specific relevance function used is determined by the relevance_function_name
            parameter provided during initialization.
            
        """
        
        for logline in self.unfiltered_data:
            if self.logline_handler.check_relevance(
                logline_dict=logline, function_name=self.relevance_function_name
            ):
                self.filtered_data.append(logline)
            else:  # not relevant, filtered out
                logline_id = uuid.UUID(logline.get("logline_id"))

                self.logline_timestamps.insert(
                    dict(
                        logline_id=logline_id,
                        stage=module_name,
                        status="filtered_out",
                        timestamp=datetime.datetime.now(),
                        is_active=False,
                    )
                )

        self.fill_levels.insert(
            dict(
                timestamp=datetime.datetime.now(),
                stage=module_name,
                entry_type="total_loglines",
                entry_count=len(self.filtered_data),
            )
        )

    def send_filtered_data(self):
        """Sends the filtered data to the configured Kafka topics.

        This method:
        1. Verifies there is filtered data to send
        2. Prepares the data in the required batch format
        3. Records completion timestamps in the database
        4. Sends the data to all configured produce topics

        Raises:
            ValueError: If there is no filtered data to send
        """
        
        row_id = generate_collisions_resistant_uuid()

        if not self.filtered_data:
            raise ValueError("Failed to send data: No filtered data.")
        data_to_send = {
            "batch_tree_row_id": row_id,
            "batch_id": self.batch_id,
            "begin_timestamp": self.begin_timestamp,
            "end_timestamp": self.end_timestamp,
            "data": self.filtered_data,
        }

        # important to finish before sending, otherwise inspector can process before finished here!
        self.batch_timestamps.insert(
            dict(
                batch_id=self.batch_id,
                stage=module_name,
                instance_name=self.name,
                status="finished",
                timestamp=datetime.datetime.now(),
                is_active=True,
                message_count=len(self.filtered_data),
            )
        )

        self.batch_tree.insert(
            dict(
                batch_row_id=row_id,
                stage=module_name,
                instance_name=self.name,
                status="finished",
                timestamp=datetime.datetime.now(),
                parent_batch_row_id=self.parent_row_id,
                batch_id=self.batch_id,
            )
        )

        self.fill_levels.insert(
            dict(
                timestamp=datetime.datetime.now(),
                stage=module_name,
                entry_type="total_loglines",
                entry_count=0,
            )
        )

        batch_schema = marshmallow_dataclass.class_schema(Batch)()
        for topic in self.produce_topics:

            self.kafka_produce_handler.produce(
                topic=topic,
                data=batch_schema.dumps(data_to_send),
                key=self.subnet_id,
            )

        logger.info(
            f"Filtered data was successfully sent:\n"
            f"    ⤷  Contains data field of {len(self.filtered_data)} message(s). Originally: "
            f"{len(self.unfiltered_data)} message(s). Belongs to subnet_id '{self.subnet_id}'."
        )

    def clear_data(self):
        """Clears the data in the internal data structures."""
        self.unfiltered_data = []
        self.filtered_data = []

    def bootstrap_prefiltering_process(self):
        """Runs the main prefiltering process loop.

        This method implements an infinite loop that:
        1. Fetches new data from Kafka
        2. Filters the data for relevance
        3. Sends the filtered data to inspectors

        """
        logger.info(f"I am {self.consume_topic}")
        counter = 0
        while True:
            self.get_and_fill_data()
            self.check_data_relevance_using_rules()
            self.send_filtered_data()
            counter += 1

    async def start(self): # pragma: no cover
        """Starts the ``Prefilter`` processing loop.

        This method:
        1. Logs startup information
        2. Runs the main processing loop in a separate thread
        3. Handles graceful shutdown on interruption

        Args:
            one_iteration (bool): If True, processes only one batch (for testing). Default: False

        """
        loop = asyncio.get_running_loop()
        logger.info(
            "Prefilter started:\n"
            f"    ⤷  receiving on Kafka topic '{self.consume_topic}'"
        )
        await loop.run_in_executor(None, self.bootstrap_prefiltering_process)
        logger.info("Closing down Prefilter...")
        self.clear_data()


async def main() -> None:
    """Creates and starts all configured Prefilter instances.

    This function:
    1. Iterates through all prefilter configurations defined in config.yaml
    2. For each prefilter:
        - Determines the relevance function to use
        - Sets up the validation configuration based on the config.yaml
        - Determines the topics to consume from and produce to
        - Creates an according ``Prefilter`` instance
        - Runs the ``start`` method
        
    """
    tasks = []
    for prefilter in PREFILTERS:
        relevance_function_name = prefilter["relevance_method"]
        validation_config = [
            item
            for collector in COLLECTORS
            if collector["name"] == prefilter["collector_name"]
            for item in collector["required_log_information"]
        ]
        consume_topic = f"{CONSUME_TOPIC_PREFIX}-{prefilter['name']}"
        produce_topics = [
            f"{PRODUCE_TOPIC_PREFIX}-{inspector['name']}"
            for inspector in INSPECTORS
            if prefilter["name"] == inspector["prefilter_name"]
        ]
        prefilter_instance = Prefilter(
            validation_config=validation_config,
            consume_topic=consume_topic,
            produce_topics=produce_topics,
            relevance_function_name=relevance_function_name,
        )
        prefilter_instance.name = prefilter["name"]
        tasks.append(asyncio.create_task(prefilter_instance.start()))
    await asyncio.gather(*tasks)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
