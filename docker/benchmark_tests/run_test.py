import argparse
import datetime
import ipaddress
import os
import random
import sys
import time

import polars as pl
from confluent_kafka import KafkaError

sys.path.append(os.getcwd())
from src.base.kafka_handler import SimpleKafkaProduceHandler
from src.train.dataset import Dataset, DatasetLoader
from src.base.log_config import get_logger
from src.base.utils import setup_config

logger = get_logger()
config = setup_config()

PRODUCE_TO_TOPIC = config["environment"]["kafka_topics"]["pipeline"]["logserver_in"]


class DatasetGenerator:
    """Generates log lines and datasets."""

    def __init__(self, data_base_path: str = "./data"):
        datasets = DatasetLoader(base_path=data_base_path, max_rows=10000)

        dataset = Dataset(
            name="",
            data_path="",
            data=pl.concat(
                [
                    datasets.dgta_dataset.data,
                    # datasets.cic_dataset.data,
                    # datasets.bambenek_dataset.data,
                    # datasets.dga_dataset.data,
                    # datasets.dgarchive_dataset.data,
                ]
            ),
            max_rows=1000,
        )

        self.domains = dataset.data

    def generate_random_logline(
        self, statuses: list[str] = None, record_types: list[str] = None
    ):
        """Generates a (mostly) random logline."""
        if record_types is None:
            record_types = 6 * ["AAAA"] + 10 * ["A"] + ["PR", "CNAME"]

        if statuses is None:
            statuses = ["NOERROR", "NXDOMAIN"]

        # choose timestamp
        timestamp = (
            datetime.datetime.now() + datetime.timedelta(0, 0, random.randint(0, 900))
        ).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        # choose status code
        status = random.choice(statuses)

        # choose client IP address
        number_of_subnets = 50
        src_ip = (
            f"192.168.{random.randint(0, number_of_subnets)}.{random.randint(1, 255)}"
        )

        # choose server IP address
        server_ip = f"10.10.0.{random.randint(1, 100)}"

        # choose random domain (can be malicious or benign)
        domain = self.get_random_domain()

        # choose random record type
        record_type = random.choice(record_types)

        # choose random response IP address
        def _get_random_ipv4():
            max_ipv4 = ipaddress.IPv4Address._ALL_ONES  # 2 ** 32 - 1
            return ipaddress.IPv4Address._string_from_ip_int(
                random.randint(0, max_ipv4)
            )

        def _get_random_ipv6():
            max_ipv6 = ipaddress.IPv6Address._ALL_ONES  # 2 ** 128 - 1
            return ipaddress.IPv6Address._string_from_ip_int(
                random.randint(0, max_ipv6)
            )

        ip_address_choices = [_get_random_ipv4(), _get_random_ipv6()]
        response_ip_address = random.choice(ip_address_choices)

        # choose random size
        size = f"{random.randint(50, 255)}b"

        return f"{timestamp} {status} {src_ip} {server_ip} {domain} {record_type} {response_ip_address} {size}"

    def get_random_domain(self) -> str:
        random_domain = self.domains.sample(n=1)
        return random_domain["query"].item()

    def generate_dataset(self, number_of_elements: int) -> list[str]:
        dataset = []

        for _ in range(number_of_elements):
            logline = self.generate_random_logline()
            dataset.append(logline)

        return dataset


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

            if msg_per_sec > 0:
                time.sleep(1.0 / msg_per_sec)
            else:
                time.sleep(1.0)

        logger.warning(f"Finish interval with {msg_per_sec} msg/s")
        return cur_index


class RampUpTest(ScalabilityTest):
    """Starts with a low rate and increases the rate in fixed intervals."""

    def __init__(
        self,
        msg_per_sec_in_intervals: list[float | int],
        interval_length_in_sec: int | float | list[int | float],
    ):
        super().__init__()
        self.msg_per_sec_in_intervals = msg_per_sec_in_intervals

        if type(interval_length_in_sec) is list:
            self.interval_lengths = interval_length_in_sec
        else:
            self.interval_lengths = [
                interval_length_in_sec for _ in range(len(msg_per_sec_in_intervals))
            ]

        if len(interval_length_in_sec) != len(msg_per_sec_in_intervals):
            raise Exception("Different lengths of interval lists. Must be equal.")


class BurstTest(ScalabilityTest):
    """Starts with a normal rate, sends a high rate for a short period, then returns to normal rate. Repeats the
    process for a defined number of times."""

    def __init__(
        self,
        normal_rate_msg_per_sec: float | int,
        burst_rate_msg_per_sec: float | int,
        normal_rate_interval_length: float | int,
        burst_rate_interval_length: float | int,
        number_of_intervals: int = 1,
    ):
        super().__init__()

        self.msg_per_sec_in_intervals = [normal_rate_msg_per_sec]
        self.interval_lengths = [normal_rate_interval_length]

        for _ in range(number_of_intervals):
            self.msg_per_sec_in_intervals.append(burst_rate_msg_per_sec)
            self.msg_per_sec_in_intervals.append(normal_rate_msg_per_sec)

            self.interval_lengths.append(burst_rate_interval_length)
            self.interval_lengths.append(normal_rate_interval_length)


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


class MaximumThroughputTest(LongTermTest):
    """Keeps a consistent rate that is too high to be handled."""

    def __init__(self, length_in_min: float | int, msg_per_sec: int = 500):
        super().__init__(full_length_in_min=length_in_min, msg_per_sec=msg_per_sec)


def main():
    # Get the environment variable, default to 1 if not set
    env_test_type_nr = int(os.getenv("TEST_TYPE_NR", 1))

    parser = argparse.ArgumentParser(
        description="Example script with test_type_nr argument."
    )

    parser.add_argument(
        "--test_type_nr",
        type=int,
        choices=[1, 2, 3, 4],
        default=env_test_type_nr,
        help="""
        1 - Ramp-up test
        2 - Burst test
        3 - Maximum throughput test
        4 - Long-term test
        """,
    )

    args = parser.parse_args()

    print(f"Selected test type number: {args.test_type_nr}")
    test_type_nr = args.test_type_nr

    """Creates the test instance and executes the test."""
    match test_type_nr:
        case 1:
            ramp_up_test = RampUpTest(
                msg_per_sec_in_intervals=[10, 50, 100, 150],
                interval_length_in_sec=[120, 120, 120, 120],
            )
            ramp_up_test.execute()

        case 2:
            burst_test = BurstTest(
                normal_rate_msg_per_sec=50,
                burst_rate_msg_per_sec=1000,
                normal_rate_interval_length=120,
                burst_rate_interval_length=2,
                number_of_intervals=3,
            )
            burst_test.execute()

        case 3:
            maximum_throughput_test = MaximumThroughputTest(
                length_in_min=5,
            )
            maximum_throughput_test.execute()

        case 4:
            long_term_test = LongTermTest(
                full_length_in_min=10,
                msg_per_sec=15,
            )
            long_term_test.execute()

        case _:
            pass


if __name__ == "__main__":
    """ """
    main()
