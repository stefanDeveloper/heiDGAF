import os
import sys

sys.path.append(os.getcwd())
from benchmarking.src.test_types.base import SingleIntervalTest
from src.base.log_config import get_logger

logger = get_logger()


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
