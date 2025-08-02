import argparse
import os
import sys

sys.path.append(os.getcwd())
from benchmarking.src.test_types.base import IntervalBasedTest
from src.base.log_config import get_logger
from benchmarking.src.setup_config import setup_config

logger = get_logger()
benchmark_test_config = setup_config()

burst_test_config = benchmark_test_config["tests"]["burst"]


class BurstTest(IntervalBasedTest):
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
        interval_lengths_in_seconds = [normal_rate_interval_length]
        messages_per_second_in_intervals = [normal_rate_msg_per_sec]

        for _ in range(number_of_repetitions):
            messages_per_second_in_intervals.append(burst_rate_msg_per_sec)
            messages_per_second_in_intervals.append(normal_rate_msg_per_sec)

            interval_lengths_in_seconds.append(burst_rate_interval_length)
            interval_lengths_in_seconds.append(normal_rate_interval_length)

        super().__init__(
            interval_lengths_in_seconds=interval_lengths_in_seconds,
            messages_per_second_in_intervals=messages_per_second_in_intervals,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Execute the burst test with given test parameters. "
        "By default, configuration file values are used."
    )

    parser.add_argument(
        "--normal_data_rate",
        type=float,
        help=f"Normal Rate Test: data rate in msg/s [float | int], "
        f"default: {burst_test_config['normal_rate']['data_rate']}",
        default=burst_test_config["normal_rate"]["data_rate"],
    )
    parser.add_argument(
        "--normal_interval_length",
        type=float,
        help=f"Normal Rate Test: interval length in seconds [float | int], "
        f"default: {burst_test_config['normal_rate']['interval_length']}",
        default=burst_test_config["normal_rate"]["interval_length"],
    )
    parser.add_argument(
        "--burst_data_rate",
        type=float,
        help=f"Burst Rate Test: data rate in msg/s [float | int], "
        f"default: {burst_test_config['burst_rate']['data_rate']}",
        default=burst_test_config["burst_rate"]["data_rate"],
    )
    parser.add_argument(
        "--burst_interval_length",
        type=float,
        help=f"Burst Rate Test: interval length in seconds [float | int], "
        f"default: {burst_test_config['burst_rate']['interval_length']}",
        default=burst_test_config["burst_rate"]["interval_length"],
    )
    parser.add_argument(
        "--number_of_repetitions",
        type=int,
        help=f"number of intervals [int], default: {burst_test_config['number_of_repetitions']}",
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
