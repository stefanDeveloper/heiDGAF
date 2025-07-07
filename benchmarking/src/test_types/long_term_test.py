import argparse
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
from benchmarking.src.setup_config import setup_config as setup_benchmark_test_config

logger = get_logger()
config = setup_config()
benchmark_test_config = setup_benchmark_test_config()

long_term_test_config = benchmark_test_config["tests"]["long_term"]
PRODUCE_TO_TOPIC = config["environment"]["kafka_topics"]["pipeline"]["logserver_in"]


class LongTermTest:
    """Keeps a consistent rate for a long time."""

    def __init__(self, full_length_in_min: float | int, msg_per_sec: float | int):
        self.dataset_generator = DatasetGenerator()
        self.kafka_producer = SimpleKafkaProduceHandler()

        self.msg_per_sec = msg_per_sec
        self.full_length_in_min = full_length_in_min
        self.progress_bar = None
        self.__text_message_count = None

    def execute(self):
        """Executes the test with the configured parameters."""
        start_timestamp = datetime.datetime.now()
        logger.info(
            f"Start {self.full_length_in_min} minute-test with "
            f"rate {self.msg_per_sec} msg/sec at: {start_timestamp}"
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
                "Sent: ",
                self.__text_message_count,
                f"/{self._get_total_message_count()}",
            ],
        )
        self.progress_bar.start()

        cur_index = 0
        while datetime.datetime.now() - start_timestamp < datetime.timedelta(
            minutes=self.full_length_in_min
        ):
            try:
                self.kafka_producer.produce(
                    PRODUCE_TO_TOPIC,
                    self.dataset_generator.generate_random_logline(),
                )

                self.__text_message_count.update_mapping(cur_message_count=cur_index)
                self.progress_bar.update(
                    min(
                        self._get_time_elapsed()
                        / datetime.timedelta(minutes=self.full_length_in_min)
                        * 100,
                        100,
                    )
                )

                cur_index += 1
            except KafkaError:
                logger.warning(KafkaError)
            time.sleep(1.0 / self.msg_per_sec)

        self.progress_bar.update(100)
        self.progress_bar.finish()

        logger.info(f"Stop at: {datetime.datetime.now()}")

    def _get_time_elapsed(self) -> datetime.timedelta:
        return datetime.datetime.now() - self.progress_bar.start_time

    def _get_total_message_count(self):
        return round(self.msg_per_sec * self.full_length_in_min * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Execute the long term test with given test parameters. By default, configuration file values are used."
    )

    parser.add_argument(
        "--data_rate",
        type=float,
        help=f"Data rate in msg/s [float | int], default: {long_term_test_config['data_rate']}",
        default=long_term_test_config["data_rate"],
    )
    parser.add_argument(
        "--length",
        type=float,
        help=f"Full length/duration in minutes [float | int], default: {long_term_test_config['length']}",
        default=long_term_test_config["length"],
    )

    args = parser.parse_args()

    long_term_test = LongTermTest(
        full_length_in_min=args.length,
        msg_per_sec=args.data_rate,
    )
    long_term_test.execute()
