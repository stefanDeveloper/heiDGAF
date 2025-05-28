import os
import sys

sys.path.append(os.getcwd())
from benchmarking.src.test_types.scalability_test import ScalabilityTest
from src.base.log_config import get_logger
from benchmarking.src.setup_config import setup_config

logger = get_logger()
benchmark_test_config = setup_config()

ramp_up_test_config = benchmark_test_config["ramp_up"]


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
    data_rates = []
    interval_lengths = []

    for e in ramp_up_test_config["intervals"]:
        data_rates.append(e[0])
        interval_lengths.append(e[1])

    ramp_up_test = RampUpTest(
        msg_per_sec_in_intervals=data_rates,
        interval_length_in_sec=interval_lengths,
    )
    ramp_up_test.execute()
