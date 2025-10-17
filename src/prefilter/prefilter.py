import datetime
import os
import sys
import uuid

import marshmallow_dataclass

sys.path.append(os.getcwd())
from src.base.clickhouse_kafka_sender import ClickHouseKafkaSender
from src.base.data_classes.batch import Batch
from src.base.logline_handler import LoglineHandler
from src.base.kafka_handler import (
    ExactlyOnceKafkaConsumeHandler,
    ExactlyOnceKafkaProduceHandler,
    KafkaMessageFetchException,
)
from src.base.log_config import get_logger
from src.base.utils import setup_config

module_name = "log_filtering.prefilter"
logger = get_logger(module_name)

config = setup_config()
CONSUME_TOPIC = config["environment"]["kafka_topics"]["pipeline"][
    "batch_sender_to_prefilter"
]
PRODUCE_TOPIC = config["environment"]["kafka_topics"]["pipeline"][
    "prefilter_to_inspector"
]
KAFKA_BROKERS = ",".join(
    [
        f"{broker['hostname']}:{broker['port']}"
        for broker in config["environment"]["kafka_brokers"]
    ]
)


class Prefilter:
    """Main component of the Log Filtering stage to process and filter batches

    Consumes batches from the Log Collection stage and applies relevance-based filtering
    using the :class:`LoglineHandler`. Filters out irrelevant loglines and forwards only relevant
    data to the next pipeline stage for anomaly detection.
    """

    def __init__(self):
        self.batch_id = None
        self.begin_timestamp = None
        self.end_timestamp = None
        self.subnet_id = None

        self.unfiltered_data = []
        self.filtered_data = []

        self.logline_handler = LoglineHandler()
        self.kafka_consume_handler = ExactlyOnceKafkaConsumeHandler(CONSUME_TOPIC)
        self.kafka_produce_handler = ExactlyOnceKafkaProduceHandler()

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
        """Retrieves and processes a new batch from the configured Kafka topic.

        Clears any previously stored data and consumes a new batch message. Unpacks the batch
        data including metadata (batch_id, timestamps, subnet_id) and stores it internally.
        Logs batch reception information and updates monitoring metrics for tracking purposes.
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
        """Applies relevance-based filtering to the unfiltered batch data.

        Iterates through all loglines in the unfiltered data and applies the relevance check
        using the :class:`LoglineHandler`. Relevant loglines are added to the filtered data, while
        irrelevant ones are discarded and marked as "filtered_out" in the monitoring system.
        Updates fill level metrics to track filtering progress.
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

    def send_filtered_data(self) -> None:
        """Sends the filtered batch data to the next pipeline stage via Kafka.

        Creates a properly formatted batch message with metadata and sends it to the
        configured output topic. Updates batch processing status and resets fill level
        metrics. Logs detailed statistics about the filtering results.

        Raises:
            ValueError: If no filtered data is available to send.
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
        self.kafka_produce_handler.produce(
            topic=PRODUCE_TOPIC,
            data=batch_schema.dumps(data_to_send),
            key=self.subnet_id,
        )
        logger.info(
            f"Filtered data was successfully sent:\n"
            f"    ⤷  Contains data field of {len(self.filtered_data)} message(s). Originally: "
            f"{len(self.unfiltered_data)} message(s). Belongs to subnet_id '{self.subnet_id}'."
        )

    def clear_data(self) -> None:
        """Clears all data from the internal data structures.

        Resets both unfiltered_data and filtered_data lists to empty state,
        preparing for the next batch processing cycle.
        """
        self.unfiltered_data = []
        self.filtered_data = []


def main(one_iteration: bool = False) -> None:
    """Creates the :class:`Prefilter` instance and runs the main processing loop.

    Continuously processes batches by retrieving data, applying filters, and sending
    filtered results. The loop handles various exceptions gracefully and supports
    clean shutdown via KeyboardInterrupt.

    Args:
        one_iteration (bool): If True, only processes one batch and exits.
                              Used primarily for testing purposes. Default: False
    """
    prefilter = Prefilter()

    iterations = 0
    while True:
        if one_iteration and iterations > 0:
            break
        iterations += 1

        try:
            prefilter.get_and_fill_data()
            prefilter.filter_by_error()
            prefilter.send_filtered_data()
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
            prefilter.clear_data()


if __name__ == "__main__":  # pragma: no cover
    main()
