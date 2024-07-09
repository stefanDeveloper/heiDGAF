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
    def __init__(self, error_type: str):
        self.begin_timestamp = None
        self.end_timestamp = None
        self.unfiltered_data = []
        self.filtered_data = []
        self.error_type = error_type

        logger.debug(f"Calling KafkaProduceHandler(transactional_id='prefilter')...")
        self.kafka_produce_handler = KafkaProduceHandler(transactional_id='prefilter')
        logger.debug(f"Calling KafkaConsumeHandler(topic='Prefilter')...")
        self.kafka_consume_handler = KafkaConsumeHandler(topic="Prefilter")

    def get_and_fill_data(self):
        if self.unfiltered_data:
            logger.warning("Overwriting existing data by new message.")
        self.clear_data()

        data = self.kafka_consume_handler.consume_and_return_json_data()
        # TODO: Check data
        if data:
            self.begin_timestamp = data["begin_timestamp"]
            self.end_timestamp = data["end_timestamp"]
            self.unfiltered_data = data["data"]
        logger.debug("Received consumer message as json data.")

    def filter_by_error(self):
        for e in self.unfiltered_data:
            e_as_json = ast.literal_eval(e)
            if e_as_json["status"] == self.error_type:
                self.filtered_data.append(e)

    def send_filtered_data(self):
        if not self.filtered_data:
            raise ValueError("Failed to send data: No filtered data.")

        data_to_send = {
            "begin_timestamp": self.begin_timestamp,
            "end_timestamp": self.end_timestamp,
            "data": self.filtered_data,
        }
        self.kafka_produce_handler.send(
            topic="Inspect",
            data=json.dumps(data_to_send),
        )
        logger.info(f"Sent filtered data with time frame from {self.begin_timestamp} to {self.end_timestamp} and data "
                    f"({len(self.filtered_data)} message(s))")

    def clear_data(self):
        self.unfiltered_data = []
        self.filtered_data = []


# TODO: Test
def main():
    prefilter = Prefilter(error_type="NXDOMAIN")

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
            logger.debug("Cleared data")


if __name__ == "__main__":
    main()
