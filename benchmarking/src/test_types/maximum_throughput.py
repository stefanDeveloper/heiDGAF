import os
import sys

sys.path.append(os.getcwd())
from benchmarking.src.test_types.base import SingleIntervalTest
from src.base.log_config import get_logger

logger = get_logger()

MESSAGES_PER_SECOND = 10000


class MaximumThroughputTest(SingleIntervalTest):
    """Keeps a consistent rate that is too high to be handled."""

    def __init__(
        self,
        full_length_in_seconds: float | int,
        messages_per_second: float | int = MESSAGES_PER_SECOND,
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
