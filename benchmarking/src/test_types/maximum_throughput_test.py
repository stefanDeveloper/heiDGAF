import argparse
import os
import sys

sys.path.append(os.getcwd())
from benchmarking.src.test_types.long_term_test import LongTermTest
from src.base.log_config import get_logger
from benchmarking.src.setup_config import setup_config

logger = get_logger()
benchmark_test_config = setup_config()

maximum_throughput_test_config = benchmark_test_config["tests"]["maximum_throughput"]


class MaximumThroughputTest(LongTermTest):
    """Keeps a consistent rate that is too high to be handled."""

    def __init__(self, length_in_min: float | int, msg_per_sec: int = 10):
        super().__init__(full_length_in_min=length_in_min, msg_per_sec=msg_per_sec)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Execute the maximum throughput test with given test parameters. "
        "By default, configuration file values are used."
    )

    parser.add_argument(
        "--length",
        type=float,
        help=f"Full length/duration in minutes [float | int], default: {maximum_throughput_test_config['length'] / 60}",
        default=maximum_throughput_test_config["length"] / 60,
    )

    args = parser.parse_args()

    maximum_throughput_test = MaximumThroughputTest(
        length_in_min=args.length,
    )
    maximum_throughput_test.execute()
