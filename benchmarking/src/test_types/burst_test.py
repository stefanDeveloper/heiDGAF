import os
import sys

sys.path.append(os.getcwd())
from benchmarking.src.test_types.scalability_test import ScalabilityTest
from src.base.log_config import get_logger
from benchmarking.src.setup_config import setup_config

logger = get_logger()
benchmark_test_config = setup_config()

burst_test_config = benchmark_test_config["burst"]


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
    burst_test = BurstTest(
        normal_rate_interval_length=burst_test_config["normal_rate"]["interval_length"],
        normal_rate_msg_per_sec=burst_test_config["normal_rate"]["data_rate"],
        burst_rate_interval_length=burst_test_config["burst_rate"]["interval_length"],
        burst_rate_msg_per_sec=burst_test_config["burst_rate"]["data_rate"],
        number_of_repetitions=burst_test_config["number_of_repetitions"],
    )
    burst_test.execute()
