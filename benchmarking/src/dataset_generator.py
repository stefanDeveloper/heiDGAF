import datetime
import ipaddress
import os
import random
import sys

import polars as pl

sys.path.append(os.getcwd())
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
