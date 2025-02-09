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

logger = get_logger()


class DatasetGenerator:
    """Generates log lines and datasets."""

    def __init__(self, data_base_path: str = "./data"):
        datasets = DatasetLoader(base_path=data_base_path, max_rows=10000)

        dataset = Dataset(
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
        client_ip = (
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

        return f"{timestamp} {status} {client_ip} {server_ip} {domain} {record_type} {response_ip_address} {size}"

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


class RampUpTest(ScalabilityTest):
    """Starts with a low rate and increases the rate in fixed intervals."""

    def __init__(
        self,
        msg_per_sec_in_interval: list[float | int],
        interval_length_in_sec: int | float | list[int | float],
    ):
        super().__init__()

        self.msg_per_sec_in_interval = msg_per_sec_in_interval

        if type(interval_length_in_sec) is list:
            self.interval_lengths = interval_length_in_sec
        else:
            self.interval_lengths = [
                interval_length_in_sec for _ in range(len(msg_per_sec_in_interval))
            ]

        if len(interval_length_in_sec) != len(msg_per_sec_in_interval):
            raise Exception("Different lengths of interval lists. Must be equal.")

    def start(self):
        """Executes the ramp-up test with the configured parameters."""
        logger.warning(f"Start at: {datetime.datetime.now()}")
        cur_index = 0

        for i in range(len(self.msg_per_sec_in_interval)):
            messages_per_second = self.msg_per_sec_in_interval[i]

            start_of_interval_timestamp = datetime.datetime.now()
            logger.warning(
                f"Start interval with {messages_per_second} msg/s at {start_of_interval_timestamp}"
            )

            while (
                datetime.datetime.now() - start_of_interval_timestamp
                < datetime.timedelta(seconds=self.interval_lengths[i])
            ):
                try:
                    self.kafka_producer.produce(
                        "pipeline-logserver_in",
                        self.dataset_generator.generate_random_logline(),
                    )
                    logger.info(
                        f"Sent message {cur_index + 1} at: {datetime.datetime.now()}"
                    )
                    cur_index += 1
                except KafkaError:
                    logger.warning(KafkaError)
                time.sleep(1.0 / messages_per_second)
            logger.warning(f"Finish interval with {messages_per_second} msg/s")

        logger.warning(f"Stop at: {datetime.datetime.now()}")


class BurstTest(ScalabilityTest):
    """Starts with a normal rate, sends a high rate for a short period, then returns to normal rate."""

    def __init__(
        self,
        normal_rate_msg_per_sec: float | int,
        burst_rate_msg_per_sec: float | int,
        interval_lengths_in_sec: list[int | float],
    ):
        super().__init__()

        self.normal_rate_msg_per_sec = normal_rate_msg_per_sec
        self.burst_rate_msg_per_sec = burst_rate_msg_per_sec

        if len(interval_lengths_in_sec) != 3:
            raise Exception(
                f"Three intervals must be defined. {len(interval_lengths_in_sec)} intervals given."
            )

        self.interval_lengths = interval_lengths_in_sec

    def start(self):
        """Executes the burst test with the configured parameters."""
        cur_index = 0
        logger.warning(f"Start at: {datetime.datetime.now()}")

        # first interval (normal rate)
        start_of_interval_timestamp = datetime.datetime.now()
        logger.warning(
            f"Start normal rate interval with {self.normal_rate_msg_per_sec} msg/s at {start_of_interval_timestamp}"
        )

        while (
            datetime.datetime.now() - start_of_interval_timestamp
            < datetime.timedelta(seconds=self.interval_lengths[0])
        ):
            try:
                self.kafka_producer.produce(
                    "pipeline-logserver_in",
                    self.dataset_generator.generate_random_logline(),
                )
                logger.info(
                    f"Sent message {cur_index + 1} at: {datetime.datetime.now()}"
                )
                cur_index += 1
            except KafkaError:
                logger.warning(KafkaError)
            time.sleep(1.0 / self.normal_rate_msg_per_sec)
        logger.warning(
            f"Finish normal rate interval with {self.normal_rate_msg_per_sec} msg/s: Sent {cur_index} messages."
        )

        # second interval (burst rate)
        before_index = cur_index
        start_of_interval_timestamp = datetime.datetime.now()
        logger.warning(
            f"Start normal rate interval with {self.burst_rate_msg_per_sec} msg/s at {start_of_interval_timestamp}"
        )

        while (
            datetime.datetime.now() - start_of_interval_timestamp
            < datetime.timedelta(seconds=self.interval_lengths[1])
        ):
            try:
                self.kafka_producer.produce(
                    "pipeline-logserver_in",
                    self.dataset_generator.generate_random_logline(),
                )
                logger.info(
                    f"Sent message {cur_index + 1} at: {datetime.datetime.now()}"
                )
                cur_index += 1
            except KafkaError:
                logger.warning(KafkaError)
            time.sleep(1.0 / self.burst_rate_msg_per_sec)
        logger.warning(
            f"Finish normal rate interval with {self.burst_rate_msg_per_sec} msg/s: Sent {cur_index - before_index} messages."
        )

        # third interval (normal rate)
        before_index = cur_index
        start_of_interval_timestamp = datetime.datetime.now()
        logger.warning(
            f"Start normal rate interval with {self.normal_rate_msg_per_sec} msg/s at {start_of_interval_timestamp}"
        )

        while (
            datetime.datetime.now() - start_of_interval_timestamp
            < datetime.timedelta(seconds=self.interval_lengths[2])
        ):
            try:
                self.kafka_producer.produce(
                    "pipeline-logserver_in",
                    self.dataset_generator.generate_random_logline(),
                )
                logger.info(
                    f"Sent message {cur_index + 1} at: {datetime.datetime.now()}"
                )
                cur_index += 1
            except KafkaError:
                logger.warning(KafkaError)
            time.sleep(1.0 / self.normal_rate_msg_per_sec)
        logger.warning(
            f"Finish normal rate interval with {self.normal_rate_msg_per_sec} msg/s: Sent {cur_index - before_index} messages."
        )


class LongTermTest(ScalabilityTest):
    """Starts with a low rate and increases the rate in fixed intervals."""

    def start(self):
        full_length = 4  # in minutes
        messages_per_second = 50

        cur_index = 0
        logger.warning(f"Start at: {datetime.datetime.now()}")

        start_of_interval_timestamp = datetime.datetime.now()
        logger.warning(
            f"Start interval with {messages_per_second} msg/s at {start_of_interval_timestamp}"
        )
        while (
            datetime.datetime.now() - start_of_interval_timestamp
            < datetime.timedelta(minutes=full_length)
        ):
            try:
                self.kafka_producer.produce(
                    "pipeline-logserver_in",
                    self.dataset_generator.generate_random_logline(),
                )
                logger.info(
                    f"Sent message {cur_index + 1} at: {datetime.datetime.now()}"
                )
                cur_index += 1
            except KafkaError:
                logger.warning(KafkaError)
            time.sleep(1.0 / messages_per_second)
        logger.warning(f"Finish test with {messages_per_second} msg/s")

        logger.warning(f"Stop at: {datetime.datetime.now()}")


def main():
    """Creates the test instance and executes the test."""
    # ramp_up_test = RampUpTest(
    #     msg_per_sec_in_interval=[1, 10, 50, 100, 150],
    #     interval_length_in_sec=[10, 5, 4, 4, 2],
    # )
    # ramp_up_test.start()

    burst_test = BurstTest(
        normal_rate_msg_per_sec=20,
        burst_rate_msg_per_sec=10000,
        interval_lengths_in_sec=[60, 2, 60],
    )
    burst_test.start()

    # long_term_test = LongTermTest()
    # long_term_test.start()


if __name__ == "__main__":
    main()
