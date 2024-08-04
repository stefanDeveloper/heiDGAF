import ast
import json
import logging
import os
import sys

sys.path.append(os.getcwd())
from src.base.utils import setup_config
from src.base.kafka_handler import KafkaConsumeHandler, KafkaMessageFetchException, KafkaProduceHandler
from src.base.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

config = setup_config()
RELEVANT_TYPES = config["heidgaf"]["prefilter"]["relevant_types"]


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
            logger.warning("Overwriting existing data by new message...")
        self.clear_data()
        logger.debug("Cleared existing data.")

        logger.debug("Calling KafkaConsumeHandler for consuming JSON data...")
        key, data = self.kafka_consume_handler.consume_and_return_json_data()

        self.subnet_id = key
        if data:
            self.begin_timestamp = data.get("begin_timestamp")
            self.end_timestamp = data.get("end_timestamp")
            self.unfiltered_data = data.get("data")

        if not self.unfiltered_data:
            logger.info(f"Received message:\n"
                        f"    ⤷  Empty data field: No unfiltered data available. subnet_id: '{self.subnet_id}'")
        else:
            logger.info(f"Received message:\n"
                        f"    ⤷  Contains data field of {len(self.unfiltered_data)} message(s) with "
                        f"subnet_id: '{self.subnet_id}'.")

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
        logger.info("Data successfully filtered.")

    def send_filtered_data(self):
        if not self.unfiltered_data:
            logger.debug("No unfiltered or filtered data is available.")
            return

        if not self.filtered_data:
            logger.info("No errors in filtered data.")
            logger.debug("No data sent. No filtered or unfiltered data exists.")
            raise ValueError("Failed to send data: No filtered data.")

        data_to_send = {
            "begin_timestamp": self.begin_timestamp,
            "end_timestamp": self.end_timestamp,
            "data": self.filtered_data,
        }
        logger.debug("Calling KafkaProduceHandler...")
        logger.debug(f"{data_to_send=}")
        self.kafka_produce_handler.send(
            topic="Inspect_" + self.subnet_id,
            data=json.dumps(data_to_send),
            key=None,
        )
        logger.debug(f"Sent filtered data with time frame from {self.begin_timestamp} to {self.end_timestamp} and data"
                     f" ({len(self.filtered_data)} message(s)).")
        logger.info(f"Filtered data was successfully sent:\n"
                    f"    ⤷  Contains data field of {len(self.filtered_data)} message(s). Originally: "
                    f"{len(self.unfiltered_data)} message(s). Belongs to subnet_id '{self.subnet_id}'.")

    def clear_data(self):
        """
        Clears the data in the internal data structures.
        """
        self.unfiltered_data = []
        self.filtered_data = []
        logger.debug("Cleared data.")


def create_logger_str(types_list: list) -> str:
    if not types_list:
        return "Not filtering by any types."

    if len(types_list) == 1:
        return f"Filtering by type '{types_list[0]}'..."

    # Für mehr als einen Eintrag
    types_str = "', '".join(types_list[:-1])
    last_type = types_list[-1]
    return f"Filtering by types '{types_str}' and '{last_type}'..."


# TODO: Test
def main():
    logger.info("Starting Prefilter...")
    prefilter = Prefilter(error_type=RELEVANT_TYPES)
    logger.info(f"Prefilter started.\n    ⤷  {create_logger_str(RELEVANT_TYPES)}")

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
            prefilter.clear_data()


if __name__ == "__main__":
    main()
