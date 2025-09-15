import os
import sys

sys.path.append(os.getcwd())
from benchmarking.src.test_types.base import IntervalBasedTest
from src.base.log_config import get_logger

logger = get_logger()


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
