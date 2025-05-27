import datetime
import os
import sys
import time

from confluent_kafka import KafkaError

sys.path.append(os.getcwd())
from benchmarking.src.dataset_generator import DatasetGenerator
from src.base.kafka_handler import SimpleKafkaProduceHandler
from src.base.log_config import get_logger
from src.base.utils import setup_config
from benchmarking.src.setup_config import setup_config as setup_benchmark_test_config

logger = get_logger()
config = setup_config()
benchmark_test_config = setup_benchmark_test_config()

long_term_test_config = benchmark_test_config["long_term"]
PRODUCE_TO_TOPIC = config["environment"]["kafka_topics"]["pipeline"]["logserver_in"]


class LongTermTest:
    """Keeps a consistent rate for a long time."""

    def __init__(self, full_length_in_min: float | int, msg_per_sec: float | int):
        self.dataset_generator = DatasetGenerator()
        self.kafka_producer = SimpleKafkaProduceHandler()

        self.msg_per_sec = msg_per_sec
        self.full_length_in_min = full_length_in_min

    def execute(self):
        """Executes the test with the configured parameters."""
        start_timestamp = datetime.datetime.now()
        logger.warning(
            f"Start {self.full_length_in_min} minute-test with "
            f"rate {self.msg_per_sec} msg/sec at: {start_timestamp}"
        )

        cur_index = 0
        while datetime.datetime.now() - start_timestamp < datetime.timedelta(
            minutes=self.full_length_in_min
        ):
            try:
                self.kafka_producer.produce(
                    PRODUCE_TO_TOPIC,
                    self.dataset_generator.generate_random_logline(),
                )
                logger.info(
                    f"Sent message {cur_index + 1} at: {datetime.datetime.now()}"
                )
                cur_index += 1
            except KafkaError:
                logger.warning(KafkaError)
            time.sleep(1.0 / self.msg_per_sec)

        logger.warning(
            f"Stop at: {datetime.datetime.now()}, sent {cur_index} messages in the "
            f"past {(datetime.datetime.now() - start_timestamp).total_seconds() / 60} minutes."
        )


if __name__ == "__main__":
    long_term_test = LongTermTest(
        full_length_in_min=long_term_test_config["length"],
        msg_per_sec=long_term_test_config["data_rate"],
    )
