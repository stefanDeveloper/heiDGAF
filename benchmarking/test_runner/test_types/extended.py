import os
import sys
import time
from datetime import datetime, timedelta

from confluent_kafka import KafkaException

sys.path.append(os.getcwd())
from src.base.utils import setup_config
from benchmarking.test_runner.test_types.base import BaseTest
from src.base.log_config import get_logger

logger = get_logger()
config = setup_config()

PRODUCE_TO_TOPIC = config["environment"]["kafka_topics"]["pipeline"]["logserver_in"]


class IntervalBasedTest(BaseTest):
    """Base class for interval-based benchmark tests."""

    def __init__(
        self,
        name: str,
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
        self.messages_per_second_in_intervals = messages_per_second_in_intervals
        self.interval_lengths_in_seconds = self.__normalize_intervals(
            interval_lengths_in_seconds
        )

        self.__validate_interval_data()

        super().__init__(
            name=name,
            is_interval_based=True,
            total_message_count=self.__get_total_message_count(),
        )

    def _execute_core(self):
        """Executes the test by repeatedly executing single intervals.
        Updates the progress bar's interval information."""
        current_index = 0
        for i in range(len(self.messages_per_second_in_intervals)):
            self.custom_fields["interval"].update_mapping(interval_number=i + 1)
            current_index = self.__execute_single_interval(
                current_index=current_index,
                messages_per_second=self.messages_per_second_in_intervals[i],
                length_in_seconds=self.interval_lengths_in_seconds[i],
            )

    def __execute_single_interval(
        self,
        current_index: int,
        messages_per_second: float | int,
        length_in_seconds: float | int,
    ) -> int:
        """Executes a single interval and updates the progress bar accordingly.

        Args:
            current_index (int): Index of the current iteration
            messages_per_second (float | int): Data rate of the current iteration
            length_in_seconds (float | int): Interval length of the current iteration

        Returns:
            Index of the iteration after this interval
        """
        start_of_interval_timestamp = datetime.now()

        while datetime.now() - start_of_interval_timestamp < timedelta(
            seconds=length_in_seconds
        ):
            try:
                self.kafka_producer.produce(
                    PRODUCE_TO_TOPIC,
                    self.dataset_generator.generate_random_logline(),
                )

                self.custom_fields["message_count"].update_mapping(
                    current_message_count=current_index
                )
                self.progress_bar.update(
                    min(
                        self._get_time_elapsed() / self.__get_total_duration() * 100,
                        100,
                    )
                )

                current_index += 1
            except KafkaException:
                logger.error(KafkaException)

            time.sleep(1.0 / messages_per_second)

        logger.info(f"Finish interval with {messages_per_second} msg/s")
        return current_index

    def __get_total_duration(self) -> timedelta:
        """
        Returns:
            Duration of the full test run as datetime.timedelta, i.e. sum of all intervals
        """
        return timedelta(seconds=sum(self.interval_lengths_in_seconds))

    def __get_total_message_count(self) -> int:
        """
        Returns:
            Expected number of messages sent throughout the entire test run, rounded to integers.
        """
        total_message_count = 0
        for i in range(len(self.interval_lengths_in_seconds)):
            total_message_count += (
                self.interval_lengths_in_seconds[i]
                * self.messages_per_second_in_intervals[i]
            )
        return round(total_message_count)

    def __normalize_intervals(
        self, intervals: float | int | list[float | int]
    ) -> list[float | int]:
        """
        Args:
            intervals (float | int | list[float | int]): Single interval length or list of interval lengths

        Returns:
            List of interval lengths. If single value was given, all entries are the same.
        """
        if type(intervals) is not list:
            intervals = [
                intervals for _ in range(len(self.messages_per_second_in_intervals))
            ]

        return intervals

    def __validate_interval_data(self):
        if len(self.interval_lengths_in_seconds) != len(
            self.messages_per_second_in_intervals
        ):
            raise ValueError("Different lengths of interval lists. Must be equal.")


class SingleIntervalTest(BaseTest):
    """Benchmark Test implementation for Long Term Test:
    Keeps a consistent rate for a specific time span."""

    def __init__(
        self,
        name: str,
        full_length_in_minutes: float | int,
        messages_per_second: float | int,
    ):
        """
        Args:
            full_length_in_minutes (float | int): Duration in minutes for which to send messages
            messages_per_second (float | int): Number of messages per second when sending messages
        """
        self.messages_per_second = messages_per_second
        self.full_length_in_minutes = full_length_in_minutes

        super().__init__(
            name=name,
            is_interval_based=False,
            total_message_count=self.__get_total_message_count(),
        )

    def _execute_core(self):
        """Produces messages for the specified duration and updates the
        progress bar accordingly."""
        start_timestamp = datetime.now()
        current_index = 0

        while datetime.now() - start_timestamp < timedelta(
            minutes=self.full_length_in_minutes
        ):
            try:
                self.kafka_producer.produce(
                    PRODUCE_TO_TOPIC,
                    self.dataset_generator.generate_random_logline(),
                )

                self.custom_fields["message_count"].update_mapping(
                    current_message_count=current_index
                )
                self.progress_bar.update(
                    min(
                        self._get_time_elapsed()
                        / timedelta(minutes=self.full_length_in_minutes)
                        * 100,
                        100,
                    )  # current time elapsed relative to full duration
                )

                current_index += 1
            except KafkaException:
                logger.error(KafkaException)
            time.sleep(1.0 / self.messages_per_second)

        self.progress_bar.update(100)

    def __get_total_message_count(self):
        """
        Returns:
            Expected number of messages sent throughout the entire test run, rounded to integers.
        """
        return round(self.messages_per_second * self.full_length_in_minutes * 60)
