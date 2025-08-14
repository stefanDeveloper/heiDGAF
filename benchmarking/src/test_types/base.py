import os
import sys
import time
from abc import abstractmethod
from datetime import datetime, timedelta

import progressbar
from confluent_kafka import KafkaError

sys.path.append(os.getcwd())
from benchmarking.src.dataset_generator import DatasetGenerator
from src.base.kafka_handler import SimpleKafkaProduceHandler
from src.base.utils import setup_config
from src.base.log_config import get_logger

logger = get_logger()
config = setup_config()

PRODUCE_TO_TOPIC = config["environment"]["kafka_topics"]["pipeline"]["logserver_in"]


class BaseTest:
    """Base class for any benchmark test."""

    def __init__(self, total_message_count: int, is_interval_based: bool = False):
        """
        Args:
            total_message_count: Total number of messages to be sent during full test run
            is_interval_based: True if intervals are used, False for tests without intervals
        """
        if total_message_count < 1:
            raise ValueError("Given argument 'total_message_count' must be at least 1.")

        self.custom_fields = None
        self.progress_bar = None
        self.start_timestamp = None

        self.total_message_count = total_message_count
        self.is_interval_based = is_interval_based

        self.dataset_generator = DatasetGenerator()
        self.kafka_producer = SimpleKafkaProduceHandler()

    def execute(self):
        """Executes the test with the configured parameters."""
        self.start_timestamp = datetime.now()
        logger.info(f"Start test at: {self.start_timestamp}")

        self.progress_bar, self.custom_fields = self._setup_progress_bar()

        self.progress_bar.start()
        self._execute_core()
        self.progress_bar.finish()

        self.progress_bar = None
        self.custom_fields = None
        self.start_timestamp = None

        logger.info(f"Finish test at: {datetime.now()}")

    @abstractmethod
    def _execute_core(self):
        """Actual test execution inside the execution environment.
        To be implemented by inheriting classes."""
        raise NotImplementedError

    def _setup_progress_bar(self):
        """
        Sets up the progressbar and returns it including its customizable fields.

        Returns:
            progressbar.Progressbar, custom_fields (dict)
        """
        custom_fields = {
            "message_count": progressbar.FormatCustomText(  # show the number of messages already sent
                f"%(current_message_count){len(str(self.total_message_count))}s",
                dict(current_message_count=0),
            ),
        }
        if self.is_interval_based:  # show the current interval number
            custom_fields["interval"] = progressbar.FormatCustomText(
                "Int.: %(interval_number)s, ", dict(interval_number=1)
            )
        else:
            custom_fields["interval"] = progressbar.FormatCustomText("")  # empty

        progress_bar = progressbar.ProgressBar(
            maxval=100,
            widgets=[
                progressbar.Percentage(),
                " ",
                progressbar.Bar(),
                " ",
                progressbar.Timer(),
                ", ",
                custom_fields.get("interval"),  # number of current interval
                "Sent: ",
                custom_fields["message_count"],  # number of sent messages
                f"/{self.total_message_count}",  # total number of messages to be sent
            ],
        )

        return progress_bar, custom_fields

    def _get_time_elapsed(self) -> timedelta:
        """
        Returns:
            Time elapsed as datetime.timedelta since the start of the test
        """
        return datetime.now() - self.progress_bar.start_time


class IntervalBasedTest(BaseTest):
    """Base class for interval-based benchmark tests."""

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
        self.interval_lengths_in_seconds = interval_lengths_in_seconds
        self.messages_per_second_in_intervals = messages_per_second_in_intervals

        self.__handle_single_interval_value()
        self.__validate_interval_data()

        super().__init__(
            is_interval_based=True, total_message_count=self.__get_total_message_count()
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
            Index of the current iteration, same as index from input
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
            except KafkaError:
                logger.error(KafkaError)
            time.sleep(1.0 / messages_per_second)

        logger.info(f"Finish interval with {messages_per_second} msg/s")
        return current_index

    def __get_total_duration(self) -> timedelta:
        """
        Returns:
            Duration of the full test run as datetime.timedelta, i.e. sum of all intervals
        """
        return timedelta(seconds=sum(self.interval_lengths_in_seconds))

    def __get_total_message_count(self):
        """
        Returns:
            Expected number of messages sent throughout the entire test run.
        """
        total_message_count = 0
        for i in range(len(self.interval_lengths_in_seconds)):
            total_message_count += (
                self.interval_lengths_in_seconds[i]
                * self.messages_per_second_in_intervals[i]
            )
        return total_message_count

    def __handle_single_interval_value(self):
        if type(self.interval_lengths_in_seconds) is not list:
            self.interval_lengths_in_seconds = [
                self.interval_lengths_in_seconds
                for _ in range(len(self.messages_per_second_in_intervals))
            ]

    def __validate_interval_data(self):
        if len(self.interval_lengths_in_seconds) != len(
            self.messages_per_second_in_intervals
        ):
            raise ValueError("Different lengths of interval lists. Must be equal.")


class SingleIntervalTest(BaseTest):
    """Benchmark Test implementation for Long Term Test:
    Keeps a consistent rate for a specific time span."""

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
            is_interval_based=False,
            total_message_count=self.__get_total_message_count(),
        )

    def _execute_core(self):
        """Produces messages for the specified duration and updates the
        progress bar accordingly."""
        current_index = 0
        while datetime.now() - self.start_timestamp < timedelta(
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
            except KafkaError:
                logger.error(KafkaError)
            time.sleep(1.0 / self.messages_per_second)

        self.progress_bar.update(100)

    def __get_total_message_count(self):
        """
        Returns:
            Expected number of messages sent throughout the entire test run, rounded to integers.
        """
        return round(self.messages_per_second * self.full_length_in_minutes * 60)
