import argparse
import os
import sys

sys.path.append(os.getcwd())
from benchmarking.src.test_types.scalability_test import ScalabilityTest
from src.base.log_config import get_logger
from benchmarking.src.setup_config import setup_config

logger = get_logger()
benchmark_test_config = setup_config()

burst_test_config = benchmark_test_config["tests"]["burst"]


class BurstTest(ScalabilityTest):
    """Starts with a normal rate, sends a high rate for a short period, then returns to normal rate.
    Repeats the process for a defined number of times."""

    def __init__(
        self,
        normal_rate_msg_per_sec: float | int,
        burst_rate_msg_per_sec: float | int,
        normal_rate_interval_length: float | int,
        burst_rate_interval_length: float | int,
        number_of_repetitions: int = 1,
    ):
        super().__init__()

        self.msg_per_sec_in_intervals = [normal_rate_msg_per_sec]
        self.interval_lengths = [normal_rate_interval_length]

        for _ in range(number_of_repetitions):
            self.msg_per_sec_in_intervals.append(burst_rate_msg_per_sec)
            self.msg_per_sec_in_intervals.append(normal_rate_msg_per_sec)

            self.interval_lengths.append(burst_rate_interval_length)
            self.interval_lengths.append(normal_rate_interval_length)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Execute the burst test with given test parameters."
    )

    parser.add_argument(
        "--normal_data_rate",
        help="Normal Rate Test: data rate",
        default=burst_test_config["normal_rate"]["data_rate"],
    )
    parser.add_argument(
        "--normal_interval_length",
        help="Normal Rate Test: interval length",
        default=burst_test_config["normal_rate"]["interval_length"],
    )
    parser.add_argument(
        "--burst_data_rate",
        help="Burst Rate Test: data rate",
        default=burst_test_config["burst_rate"]["data_rate"],
    )
    parser.add_argument(
        "--burst_interval_length",
        help="Burst Rate Test: interval length",
        default=burst_test_config["burst_rate"]["interval_length"],
    )
    parser.add_argument(
        "--number_of_repetitions",
        type=int,
        help="Number of Intervals",
        default=burst_test_config["number_of_repetitions"],
    )

    args = parser.parse_args()

    burst_test = BurstTest(
        normal_rate_interval_length=args.normal_interval_length,
        normal_rate_msg_per_sec=args.normal_data_rate,
        burst_rate_interval_length=args.burst_interval_length,
        burst_rate_msg_per_sec=args.burst_data_rate,
        number_of_repetitions=args.number_of_repetitions,
    )
    burst_test.execute()
