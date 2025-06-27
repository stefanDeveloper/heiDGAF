import datetime
import os
import sys
import time

import progressbar
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

        self.interval_lengths = None  # in seconds
        self.msg_per_sec_in_intervals = None
        self.progress_bar = None
        self.__text_message_count = None

    def execute(self):
        """Executes the test with the configured parameters."""
        logger.warning(f"Start at: {datetime.datetime.now()}")

        text_interval = progressbar.FormatCustomText(
            "%(interval_nr)s", dict(interval_nr=1)
        )
        self.__text_message_count = progressbar.FormatCustomText(
            f"%(cur_message_count){len(str(self._get_total_message_count()))}s",
            dict(cur_message_count=0),
        )  # adjusted to correct width

        self.progress_bar = progressbar.ProgressBar(
            maxval=100,
            widgets=[
                progressbar.Percentage(),
                " ",
                progressbar.Bar(),
                " ",
                progressbar.Timer(),
                ", ",
                "Int.: ",
                text_interval,
                ", ",
                "Sent: ",
                self.__text_message_count,
                f"/{self._get_total_message_count()}",
            ],
        )
        self.progress_bar.start()

        cur_index = 0
        for i in range(len(self.msg_per_sec_in_intervals)):
            text_interval.update_mapping(interval_nr=i + 1)
            cur_index = self._execute_one_interval(
                cur_index=cur_index,
                msg_per_sec=self.msg_per_sec_in_intervals[i],
                length_in_sec=self.interval_lengths[i],
            )

        self.progress_bar.finish()

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

                self.__text_message_count.update_mapping(cur_message_count=cur_index)
                self.progress_bar.update(
                    min(
                        self._get_time_elapsed() / self._get_total_duration() * 100, 100
                    )
                )

                cur_index += 1
            except KafkaError:
                logger.warning(KafkaError)
            time.sleep(1.0 / msg_per_sec)

        logger.warning(f"Finish interval with {msg_per_sec} msg/s")
        return cur_index

    def _get_time_elapsed(self) -> datetime.timedelta:
        return datetime.datetime.now() - self.progress_bar.start_time

    def _get_total_duration(self) -> datetime.timedelta:
        return datetime.timedelta(seconds=sum(self.interval_lengths))

    def _get_total_message_count(self):
        total_message_count = 0
        for i in range(len(self.interval_lengths)):
            total_message_count += (
                self.interval_lengths[i] * self.msg_per_sec_in_intervals[i]
            )
        return total_message_count
