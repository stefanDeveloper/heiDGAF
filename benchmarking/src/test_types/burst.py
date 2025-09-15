import os
import sys

sys.path.append(os.getcwd())
from benchmarking.src.test_types.base import IntervalBasedTest
from src.base.log_config import get_logger

logger = get_logger()


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
            name="Burst",
            interval_lengths_in_seconds=interval_lengths_in_seconds,
            messages_per_second_in_intervals=messages_per_second_in_intervals,
        )
