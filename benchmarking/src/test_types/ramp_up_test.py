import argparse
import os
import sys

sys.path.append(os.getcwd())
from benchmarking.src.test_types.scalability_test import ScalabilityTest
from src.base.log_config import get_logger
from benchmarking.src.setup_config import setup_config

logger = get_logger()
benchmark_test_config = setup_config()

ramp_up_test_config = benchmark_test_config["tests"]["ramp_up"]


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

    ramp_up_test = RampUpTest(  # TODO: Check handling of single value
        msg_per_sec_in_intervals=[int(e) for e in args.data_rates.split(",")],
        interval_length_in_sec=[int(e) for e in args.durations.split(",")],
    )
    ramp_up_test.execute()
