import json
import logging
import os  # needed for Terminal execution
import sys  # needed for Terminal execution

sys.path.append(os.getcwd())  # needed for Terminal execution
from heidgaf_core.kafka_handler import KafkaConsumeHandler, KafkaMessageFetchException
from heidgaf_core.batch_handler import KafkaBatchSender
from heidgaf_core.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


class Prefilter:
    # TODO: Test
    def __init__(self, error_type: str):
        self.unfiltered_data = []
        self.filtered_data = []
        self.error_type = error_type

        self.batch_handler = KafkaBatchSender(topic="Inspect", transactional_id="prefilter")
        self.kafka_consume_handler = KafkaConsumeHandler(topic='Prefilter')

    # TODO: Test
    def get_data(self):
        if self.unfiltered_data:
            logger.warning("Overwriting existing data by new message.")

        self.clear_data()

        self.unfiltered_data = self.kafka_consume_handler.consume_and_return_json_data()
        logger.debug("Received consumer message as json data.")

    def filter_by_error(self):
        for e in self.unfiltered_data:
            if e["status"] == self.error_type:
                self.filtered_data.append(e)

    def add_filtered_data_to_batch(self):
        if not self.filtered_data:
            raise ValueError("Failed to add data to batch: No filtered data.")

        self.batch_handler.add_message(json.dumps(self.filtered_data))

    def clear_data(self):
        self.unfiltered_data = []
        self.filtered_data = []


# TODO: Test
def main():
    prefilter = Prefilter(error_type="NXDOMAIN")

    while True:
        try:
            logger.debug("Before consuming and extracting")
            prefilter.get_data()
            logger.debug("Before filtering by error")
            prefilter.filter_by_error()
            logger.debug("Before adding filtered data to batch")
            prefilter.add_filtered_data_to_batch()
        except IOError as e:
            logger.error(e)
            raise
        except ValueError as e:
            logger.debug(e)
        except KafkaMessageFetchException as e:
            logger.debug(e)
            continue
        except KeyboardInterrupt:
            logger.info("Closing down InspectPrefilter.")
            break
        finally:
            prefilter.clear_data()


if __name__ == '__main__':
    main()
