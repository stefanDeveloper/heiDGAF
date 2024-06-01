import ast
import json
import logging
import os
import sys

from confluent_kafka import Consumer

sys.path.append(os.getcwd())  # needed for Terminal execution
from heidgaf_log_collector.batch_handler import KafkaBatchSender
from heidgaf_log_collector.config import *
from heidgaf_log_collector.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


class KafkaMessageFetchError(Exception):
    pass


class InspectPrefilter:
    def __init__(self, error_type: str):
        self.unfiltered_data = []
        self.filtered_data = []
        self.error_type = error_type

        self.batch_handler = KafkaBatchSender(topic="Inspect")
        self.batch_handler.start_kafka_producer()

        self.kafka_consumer = None
        self.conf = {
            'bootstrap.servers': f"{KAFKA_BROKER_HOST}:{KAFKA_BROKER_PORT}",
            'group.id': "my_group",  # TODO: Do something with this
            'auto.offset.reset': 'earliest'  # TODO: Do something with this
        }

    def start_kafka_consumer(self):
        if self.kafka_consumer:
            logger.warning(f"Kafka Consumer already running!")
            return

        self.kafka_consumer = Consumer(self.conf)
        self.kafka_consumer.subscribe(['Prefilter'])

    # TODO: Test
    def consume_and_extract_data(self):
        message = self.kafka_consumer.poll(timeout=1.0)

        if not message:
            raise KafkaMessageFetchError("No message fetched from Kafka broker.")

        if message.error():
            raise IOError(f"An error occurred while consuming: {message.error()}")

        decoded_message = message.value().decode('utf-8')
        json_from_message = json.loads(decoded_message)

        if self.unfiltered_data:
            logger.warning("Overwriting existing data by new message.")

        for e in json_from_message:
            self.unfiltered_data.append(ast.literal_eval(e))

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
    prefilter = InspectPrefilter(error_type="NXDOMAIN")
    prefilter.start_kafka_consumer()

    while True:
        try:
            logger.debug("Before consuming and extracting")
            prefilter.consume_and_extract_data()
            logger.debug("Before filtering by error")
            prefilter.filter_by_error()
            logger.debug("Before adding filtered data to batch")
            prefilter.add_filtered_data_to_batch()
        except IOError as e:
            logger.error(e)
            raise
        except ValueError as e:
            logger.warning(e)
        except KafkaMessageFetchError as e:
            logger.debug(e)
            continue
        except KeyboardInterrupt:
            logger.info("Closing down InspectPrefilter.")
            break
        finally:
            prefilter.clear_data()


if __name__ == '__main__':
    main()
