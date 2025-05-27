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

logger = get_logger()
config = setup_config()

PRODUCE_TO_TOPIC = config["environment"]["kafka_topics"]["pipeline"]["logserver_in"]


class ScalabilityTest:
    """Base class for tests that focus on the scalability of the software."""

    def __init__(self):
        self.dataset_generator = DatasetGenerator()
        self.kafka_producer = SimpleKafkaProduceHandler()

        self.interval_lengths = None
        self.msg_per_sec_in_intervals = None

    def execute(self):
        """Executes the test with the configured parameters."""
        logger.warning(f"Start at: {datetime.datetime.now()}")

        cur_index = 0
        for i in range(len(self.msg_per_sec_in_intervals)):
            cur_index = self._execute_one_interval(
                cur_index=cur_index,
                msg_per_sec=self.msg_per_sec_in_intervals[i],
                length_in_sec=self.interval_lengths[i],
            )

        logger.warning(f"Stop at: {datetime.datetime.now()}")

    def _execute_one_interval(
        self, cur_index: int, msg_per_sec: float | int, length_in_sec: float | int
    ) -> int:
        start_of_interval_timestamp = datetime.datetime.now()
        logger.warning(
            f"Start interval with {msg_per_sec} msg/s at {start_of_interval_timestamp}"
        )

        while (
            datetime.datetime.now() - start_of_interval_timestamp
            < datetime.timedelta(seconds=length_in_sec)
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
            time.sleep(1.0 / msg_per_sec)

        logger.warning(f"Finish interval with {msg_per_sec} msg/s")
        return cur_index
