import os
import sys

import marshmallow_dataclass

sys.path.append(os.getcwd())
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

    def get_and_fill_data(self) -> None:
        """
        Clears data already stored and consumes new data. Unpacks the data and checks if it is empty. If that is the
        case, an info message is shown, otherwise the data is stored internally, including timestamps.
        """
        logger.debug("Checking for existing data...")
        if self.unfiltered_data:
            logger.warning("Overwriting existing data by new message...")
        self.clear_data()
        logger.debug("Cleared existing data.")

        logger.debug("Calling KafkaConsumeHandler for consuming JSON data...")
        key, data = self.kafka_consume_handler.consume_as_object()

        self.subnet_id = key
        if data:
            self.batch_id = data.batch_id
            self.begin_timestamp = data.begin_timestamp
            self.end_timestamp = data.end_timestamp
            self.unfiltered_data = data.data

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

        logger.debug("Received consumer message as JSON data.")
        logger.debug(f"{data=}")

    def filter_by_error(self) -> None:
        """
        Applies the filter to the data in ``unfiltered_data``, i.e. all loglines whose error status is in
        the given error types are kept and added to ``filtered_data``, all other ones are discarded.
        """
        logger.debug("Filtering data...")

        for e in self.unfiltered_data:
            if self.logline_handler.check_relevance(e):
                self.filtered_data.append(e)

        logger.debug("Data filtered and now available in filtered_data.")
        logger.info("Data successfully filtered.")

    def send_filtered_data(self):
        """
        Sends the filtered data if available via the :class:`KafkaProduceHandler`.
        """
        if not self.unfiltered_data:
            logger.debug("No unfiltered or filtered data is available.")
            return

        if not self.filtered_data:
            logger.info("No errors in filtered data.")
            logger.debug("No data sent. No filtered or unfiltered data exists.")
            raise ValueError("Failed to send data: No filtered data.")

        data_to_send = {
            "batch_id": self.batch_id,
            "begin_timestamp": self.begin_timestamp,
            "end_timestamp": self.end_timestamp,
            "data": self.filtered_data,
        }

        batch_schema = marshmallow_dataclass.class_schema(Batch)()

        logger.debug("Calling KafkaProduceHandler...")
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
        """
        Clears the data in the internal data structures.
        """
        self.unfiltered_data = []
        self.filtered_data = []
        logger.debug("Cleared data.")


def main(one_iteration: bool = False) -> None:
    """
    Runs the main loop with by

    1. Retrieving new data,
    2. Filtering the data and
    3. Sending the filtered data if not empty.

    Stops by a ``KeyboardInterrupt``, any internal data is lost.

    Args:
        one_iteration (bool): Only one iteration is done if True (for testing purposes). False by default.
    """
    logger.info("Starting Prefilter...")
    prefilter = Prefilter()
    logger.info(f"Prefilter started.")

    iterations = 0

    while True:
        if one_iteration and iterations > 0:
            break
        iterations += 1

        try:
            logger.debug("Before getting and filling data")
            prefilter.get_and_fill_data()
            logger.debug("After getting and filling data")

            logger.debug("Before filtering by error")
            prefilter.filter_by_error()
            logger.debug("After filtering by error")

            logger.debug("Before adding filtered data to batch")
            prefilter.send_filtered_data()
            logger.debug("After adding filtered data to batch")
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
