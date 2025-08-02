import argparse
import os
import sys

sys.path.append(os.getcwd())
from benchmarking.src.test_types.base_test import SingleIntervalTest
from src.base.log_config import get_logger
from benchmarking.src.setup_config import setup_config as setup_benchmark_test_config

logger = get_logger()

benchmark_test_config = setup_benchmark_test_config()
long_term_test_config = benchmark_test_config["tests"]["long_term"]


class LongTermTest(SingleIntervalTest):
    def __init__(
        self, full_length_in_minutes: float | int, messages_per_second: float | int
    ):
        """
        Args:
            full_length_in_minutes (float | int): Duration in minutes for which to send messages
            messages_per_second (float | int): Number of messages per second when sending messages
        """
        self.messages_per_second = messages_per_second
        self.full_length_in_minutes = full_length_in_minutes

        super().__init__(
            full_length_in_minutes,
            messages_per_second,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Execute the long term test with given test parameters. "
        "By default, configuration file values are used."
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
        full_length_in_minutes=args.length,
        messages_per_second=args.data_rate,
    )
    long_term_test.execute()
