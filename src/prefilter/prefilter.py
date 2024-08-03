import ast
import json
import logging
import os  # needed for Terminal execution
import sys  # needed for Terminal execution

sys.path.append(os.getcwd())  # needed for Terminal execution
from src.base.kafka_handler import KafkaConsumeHandler, KafkaMessageFetchException, KafkaProduceHandler
from src.base.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


class Prefilter:
    """
    Loads the data from the topic ``Prefilter`` and filters it so that only entries with the given status type(s) are
    kept. Filtered data is then sent using topic ``Inspect``.
    """

    def __init__(self, error_type: list[str]):
        logger.debug(f"Initializing Prefilter for {error_type=}...")
        self.begin_timestamp = None
        self.end_timestamp = None
        self.unfiltered_data = []
        self.filtered_data = []
        self.error_type = error_type
        self.subnet_id = None

        logger.debug(f"Calling KafkaProduceHandler(transactional_id='prefilter')...")
        self.kafka_produce_handler = KafkaProduceHandler(transactional_id='prefilter')
        logger.debug(f"Calling KafkaConsumeHandler(topic='Prefilter')...")
        self.kafka_consume_handler = KafkaConsumeHandler(topic="Prefilter")
        logger.debug("Initialized Prefilter.")

    def get_and_fill_data(self):
        logger.debug("Checking for existing data...")
        if self.unfiltered_data:
            logger.warning("Overwriting existing data by new message.")
        self.clear_data()
        logger.debug("Cleared existing data.")

        logger.debug("Calling KafkaConsumeHandler for consuming JSON data...")
        key, data = self.kafka_consume_handler.consume_and_return_json_data()

        self.subnet_id = key
        if data:
            self.begin_timestamp = data.get("begin_timestamp")
            self.end_timestamp = data.get("end_timestamp")
            self.unfiltered_data = data.get("data")

        logger.debug("Received consumer message as JSON data.")
        logger.debug(f"{data=}")

    def filter_by_error(self) -> None:
        """
        Applies the filter to the data in ``unfiltered_data``, i.e. all loglines whose error status is in
        the given error types are kept and added to ``filtered_data``, all other ones are discarded.
        """
        logger.debug("Filtering data...")
        for e in self.unfiltered_data:
            e_as_json = ast.literal_eval(e)
            if e_as_json["status"] in self.error_type:
                self.filtered_data.append(e)
        logger.debug("Data filtered and now available in filtered_data.")

    def send_filtered_data(self):
        if not self.filtered_data:
            logger.debug("No filtered data available.")
            if self.unfiltered_data:
                logger.debug("Unfiltered data available. Filtering data automatically...")
                self.filter_by_error()
            else:
                logger.debug("No data send. No filtered or unfiltered data exists.")
                raise ValueError("Failed to send data: No filtered data.")

        data_to_send = {
            "begin_timestamp": self.begin_timestamp,
            "end_timestamp": self.end_timestamp,
            "data": self.filtered_data,
        }
        logger.debug("Calling KafkaProduceHandler with topic='Inspect'...")
        logger.debug(f"{data_to_send=}")
        self.kafka_produce_handler.send(
            topic="Inspect_" + self.subnet_id,
            data=json.dumps(data_to_send),
            key=None,
        )
        logger.info(f"Sent filtered data with time frame from {self.begin_timestamp} to {self.end_timestamp} and data "
                    f"({len(self.filtered_data)} message(s)).")

    def clear_data(self):
        """
        Clears the data in the internal data structures.
        """
        self.unfiltered_data = []
        self.filtered_data = []
        logger.debug("Cleared data.")


# TODO: Test
def main():
    logger.info("Starting Prefilter for errors of type 'NXDOMAIN'...")
    prefilter = Prefilter(error_type=["NXDOMAIN"])
    logger.info("Prefilter started. Filtering by type 'NXDOMAIN'...")

    while True:
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
            logger.info("Closing down Prefilter...")
            prefilter.clear_data()
            logger.info("Prefilter closed down.")


if __name__ == "__main__":
    main()
