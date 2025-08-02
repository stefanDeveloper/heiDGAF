import argparse
import os
import sys

sys.path.append(os.getcwd())
from benchmarking.src.test_types.base import SingleIntervalTest
from src.base.log_config import get_logger
from benchmarking.src.setup_config import setup_config

logger = get_logger()
benchmark_test_config = setup_config()

maximum_throughput_test_config = benchmark_test_config["tests"]["maximum_throughput"]

MESSAGES_PER_SECOND = 10000


class MaximumThroughputTest(SingleIntervalTest):
    """Keeps a consistent rate that is too high to be handled."""

    def __init__(
        self,
        full_length_in_seconds: float | int,
        messages_per_second: float | int = MESSAGES_PER_SECOND,
    ):
        """
        Args:
            full_length_in_seconds (float | int): Duration in seconds for which to send messages
            messages_per_second (float | int): Number of messages per second when sending messages
        """
        super().__init__(
            full_length_in_minutes=full_length_in_seconds / 60,
            messages_per_second=messages_per_second,
        )


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
        full_length_in_seconds=args.length,
    )
    maximum_throughput_test.execute()
