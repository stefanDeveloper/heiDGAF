import os
import sys

sys.path.append(os.getcwd())
from benchmarking.src.base_test_types import SingleIntervalTest, IntervalBasedTest
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
            "Long Term",
            full_length_in_minutes,
            messages_per_second,
        )


class MaximumThroughputTest(SingleIntervalTest):
    """Keeps a consistent rate that is too high to be handled."""

    def __init__(
        self,
        full_length_in_seconds: float | int,
        messages_per_second: float | int = 10000,
    ):
        """
        Args:
            full_length_in_seconds (float | int): Duration in seconds for which to send messages
            messages_per_second (float | int): Number of messages per second when sending messages
        """
        super().__init__(
            name="Maximum Throughput",
            full_length_in_minutes=full_length_in_seconds / 60,
            messages_per_second=messages_per_second,
        )


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
