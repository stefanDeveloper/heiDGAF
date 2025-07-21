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
    SimpleKafkaConsumeHandler,
    SimpleKafkaProduceHandler,
    KafkaMessageFetchException,
)
from src.base.log_config import get_logger
from src.base.utils import setup_config, get_zeek_sensor_topic_base_names

module_name = "log_filtering.prefilter"
logger = get_logger(module_name)

config = setup_config()

CONSUME_TOPIC_PREFIX = config["environment"]["kafka_topics_prefix"]["pipeline"]["batch_sender_to_prefilter"]
PRODUCE_TOPIC_PREFIX = config["environment"]["kafka_topics_prefix"]["pipeline"]["prefilter_to_inspector"]

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

    def __init__(self, validation_config, consume_topic, produce_topics):
        self.consume_topic = consume_topic
        self.produce_topics = produce_topics
        self.batch_id = None
        self.begin_timestamp = None
        self.end_timestamp = None
        self.subnet_id = None

        self.unfiltered_data = []
        self.filtered_data = []

        self.logline_handler = LoglineHandler(validation_config)
        self.kafka_consume_handler = SimpleKafkaConsumeHandler(self.consume_topic)
        self.kafka_produce_handler = SimpleKafkaProduceHandler()

        # databases
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
        """
        Clears data already stored and consumes new data. Unpacks the data and checks if it is empty. Data is stored
        internally, including timestamps.
        """
        self.clear_data()  # clear in case we already have data stored
        
        key, data = self.kafka_consume_handler.consume_as_object()
        self.subnet_id = key
        if data.data:
            self.batch_id = data.batch_id
            self.begin_timestamp = data.begin_timestamp
            self.end_timestamp = data.end_timestamp
            self.unfiltered_data = data.data
        self.batch_timestamps.insert(
            dict(
                batch_id=self.batch_id,
                stage=module_name,
                status="in_process",
                timestamp=datetime.datetime.now(),
                is_active=True,
                message_count=len(self.unfiltered_data),
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
                f"Received message:\n"
                f"    ⤷  Contains data field of {len(self.unfiltered_data)} message(s) with "
                f"subnet_id: '{self.subnet_id}'."
            )

    def filter_by_error(self) -> None:
        """
        Applies the filter to the data in ``unfiltered_data``, i.e. all loglines whose error status is in
        the given error types are kept and added to ``filtered_data``, all other ones are discarded.
        """
        for e in self.unfiltered_data:
            if self.logline_handler.check_relevance(e):
                self.filtered_data.append(e)
            else:  # not relevant, filtered out
                logline_id = uuid.UUID(e.get("logline_id"))

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
        """
        Sends the filtered data if available via the :class:`KafkaProduceHandler`.
        """
        if not self.filtered_data:
            raise ValueError("Failed to send data: No filtered data.")
        data_to_send = {
            "batch_id": self.batch_id,
            "begin_timestamp": self.begin_timestamp,
            "end_timestamp": self.end_timestamp,
            "data": self.filtered_data,
        }
        self.batch_timestamps.insert(
            dict(
                batch_id=self.batch_id,
                stage=module_name,
                status="finished",
                timestamp=datetime.datetime.now(),
                is_active=True,
                message_count=len(self.filtered_data),
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


    def start(self, one_iteration: bool = False):
        """
        Runs the main loop by

        1. Retrieving new data,
        2. Filtering the data and
        3. Sending the filtered data if not empty.

        Stops by a ``KeyboardInterrupt``, any internal data is lost.

        Args:
            one_iteration (bool): Only one iteration is done if True (for testing purposes). False by default.
        """  
        
        logger.info(
            "Prefilter started:\n"
            f"    ⤷  receiving on Kafka topic '{self.consume_topic}'"
        )          
        
        iterations = 0
        while True:
            if one_iteration and iterations > 0:
                break
            iterations += 1

            try:
                self.get_and_fill_data()
                self.filter_by_error()
                self.send_filtered_data()
            except IOError as e:
                logger.error(e)
                raise
            except ValueError as e:
                logger.debug(e)
            except KafkaMessageFetchException as e:
                logger.debug(e)
                continue
            except KeyboardInterrupt:
                logger.info("Closing down Prefilter...")
                break
            finally:
                self.clear_data()

async def main() -> None:  
    tasks = []
    for prefilter in PREFILTERS:
        validation_config = [collector["required_log_information"] for collector in COLLECTORS if collector["name"] == prefilter["collector_name"]][0]
        consume_topic = f"{CONSUME_TOPIC_PREFIX}-{prefilter['name']}"
        produce_topics = [f"{PRODUCE_TOPIC_PREFIX}-{inspector['name']}" for inspector in INSPECTORS if prefilter["name"] == inspector["prefilter_name"]]
        prefilter = Prefilter(validation_config=validation_config, consume_topic=consume_topic, produce_topics=produce_topics)
        tasks.append(
            asyncio.create_task(prefilter.start())
        )
    await asyncio.gather(*tasks)

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
