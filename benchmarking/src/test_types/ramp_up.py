import argparse
import os
import sys

sys.path.append(os.getcwd())
from benchmarking.src.test_types.base import IntervalBasedTest
from src.base.log_config import get_logger
from benchmarking.src.setup_config import setup_config

logger = get_logger()
benchmark_test_config = setup_config()

ramp_up_test_config = benchmark_test_config["tests"]["ramp_up"]


class RampUpTest(IntervalBasedTest):
    """Starts with a low rate and increases the rate in fixed intervals."""

    def __init__(
        self,
        interval_lengths_in_seconds: int | float | list[int | float],
        messages_per_second_in_intervals: list[float | int],
    ):
        """
        Args:
            interval_lengths_in_seconds: Single value to use for each interval, or list of lengths for each interval
                                         separately
            messages_per_second_in_intervals: List of message rates per interval. Must have same length as
                                              interval_lengths_in_seconds, if a list is specified there.
        """
        super().__init__(
            "Ramp Up",
            interval_lengths_in_seconds,
            messages_per_second_in_intervals,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Execute the ramp-up test with given test parameters. "
        "By default, configuration file values are used."
    )

    default_data_rates = []
    default_durations = []

    for e in ramp_up_test_config["intervals"]:
        default_data_rates.append(e[0])
        default_durations.append(e[1])

    parser.add_argument(
        "--data_rates",
        help=f"Data rates per interval in msg/s [List[float | int], "
        f"default: {','.join(str(i) for i in default_data_rates)}",
        default=",".join(str(i) for i in default_data_rates),
    )

    parser.add_argument(
        "--durations",
        help=f"Length/duration per interval in minutes [float | int | List[float | int]], "
        f"single value applies to all intervals, "
        f"default: {','.join(str(i) for i in default_durations)}",
        default=",".join(str(i) for i in default_data_rates),
    )

    args = parser.parse_args()

    ramp_up_test = RampUpTest(
        messages_per_second_in_intervals=[int(e) for e in args.data_rates.split(",")],
        interval_lengths_in_seconds=[int(e) for e in args.durations.split(",")],
    )
    ramp_up_test.execute_and_generate_report()
