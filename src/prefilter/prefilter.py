import datetime
import os
import sys

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
from src.base.utils import generate_unique_transactional_id
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
    """
    Loads the data from the topic ``Prefilter`` and filters it so that only entries with the given status type(s) are
    kept. Filtered data is then sent using topic ``Inspect``.
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
        transactional_id = generate_unique_transactional_id(module_name, KAFKA_BROKERS)
        self.kafka_produce_handler = ExactlyOnceKafkaProduceHandler(transactional_id)

        # databases
        self.batch_timestamps = ClickHouseKafkaSender("batch_timestamps")
        self.logline_status = ClickHouseKafkaSender("logline_status")
        self.logline_timestamps = ClickHouseKafkaSender("logline_timestamps")

    def get_and_fill_data(self) -> None:
        """
        Clears data already stored and consumes new data. Unpacks the data and checks if it is empty. Data is stored
        internally, including timestamps.
        """
        self.clear_data()  # clear in case we already have data stored

        key, data = self.kafka_consume_handler.consume_as_object()

        self.subnet_id = key
        if data:
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
                message_count=len(self.unfiltered_data),
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
                logline_id = e.get("logline_id")  # TODO: Check

                self.logline_timestamps.insert(
                    dict(
                        logline_id=logline_id,
                        stage=module_name,
                        status="filtered_out",
                        timestamp=datetime.datetime.now(),
                    )
                )

                self.logline_status.insert(
                    dict(
                        logline_id=logline_id,
                        is_active=False,
                        exit_at_stage=module_name,
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
                message_count=len(self.filtered_data),
            )
        )

        batch_schema = marshmallow_dataclass.class_schema(Batch)()

        logger.debug(f"{data_to_send=}")
        self.kafka_produce_handler.produce(
            topic=PRODUCE_TOPIC,
            data=batch_schema.dumps(data_to_send),
            key=self.subnet_id,
        )
        logger.debug(
            f"Sent filtered data with time frame from {self.begin_timestamp} to {self.end_timestamp} and data"
            f" ({len(self.filtered_data)} message(s))."
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


def main(one_iteration: bool = False) -> None:
    """
    Runs the main loop by

    1. Retrieving new data,
    2. Filtering the data and
    3. Sending the filtered data if not empty.

    Stops by a ``KeyboardInterrupt``, any internal data is lost.

    Args:
        one_iteration (bool): Only one iteration is done if True (for testing purposes). False by default.
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
