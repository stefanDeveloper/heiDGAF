import datetime
import ipaddress
import os
import random
import re
import subprocess
import sys
import time
from abc import abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import polars as pl
import progressbar
from confluent_kafka import KafkaException

sys.path.append(os.getcwd())
from src.base.kafka_handler import SimpleKafkaProduceHandler
from benchmarking.src.plot_generator import PlotGenerator
from src.base.utils import setup_config
from benchmarking.src.pdf_overview_generator import PDFOverviewGenerator
from src.train.dataset import Dataset, DatasetLoader
from src.base.log_config import get_logger

logger = get_logger()
config = setup_config()

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent  # heiDGAF directory
PRODUCE_TO_TOPIC = config["environment"]["kafka_topics"]["pipeline"]["logserver_in"]
LATENCIES_COMPARISON_FILENAME = "latency_comparison.png"
MODULE_TO_CSV_FILENAME = {
    "Batch Handler": "batch_handler.csv",
    "Collector": "collector.csv",
    "Detector": "detector.csv",
    "Inspector": "inspector.csv",
    "Log Server": "logserver.csv",
    "Prefilter": "prefilter.csv",
}
CLICKHOUSE_CONTAINER_NAME = config["environment"]["monitoring"]["clickhouse_server"][
    "hostname"
]


class BaseTest:
    """Base class for any benchmark test."""

    def __init__(
        self, name: str, total_message_count: int, is_interval_based: bool = False
    ):
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
        self.plot_generator = None
        self.test_run_directory: Optional[Path] = None

        self.test_name = name
        self.total_message_count = total_message_count
        self.is_interval_based = is_interval_based

        self.dataset_generator = BenchmarkDatasetGenerator()
        self.kafka_producer = SimpleKafkaProduceHandler()

    def execute(self):
        """Executes the test with the configured parameters."""
        self.start_timestamp = datetime.now()
        logger.info(f"{self.test_name}: Start test at {self.start_timestamp}")

        self.progress_bar, self.custom_fields = self._setup_progress_bar()

        self.progress_bar.start()
        self._execute_core()
        self.progress_bar.finish()

        self.progress_bar = None
        self.custom_fields = None

        logger.info(f"{self.test_name}: Finish test at {datetime.now()}")

    def execute_and_generate_report(self):
        self.__validate_filename(self.test_name)

        self.__cleanup_clickhouse_database()
        logger.info(f"{self.test_name} Preparation: Database cleanup finished")

        self.execute()
        self.__check_if_all_data_is_processed()

        file_identifier = str(
            datetime.now().strftime("%Y%m%d_%H%M%S")
            + "_"
            + self.test_name.lower().replace(" ", "_")
        )
        self.test_run_directory = Path(
            f"{BASE_DIR}/benchmark_results/{file_identifier}"
        )

        self.__extract_all_data_from_clickhouse(file_identifier)
        logger.info(
            f"{self.test_name}: Database entries extracted under {file_identifier}"
        )

        self.__cleanup_clickhouse_database()
        logger.info(f"{self.test_name} Cleanup: After-test database cleanup finished")

        self._generate_plots()
        self._generate_report()

    @abstractmethod
    def _execute_core(self):
        """Actual test execution inside the execution environment.
        To be implemented by inheriting classes."""
        raise NotImplementedError

    def _generate_plots(self):
        self.plot_generator = PlotGenerator()

        self.__plot_latency_comparison()

        self.plot_generator = None

    def _generate_report(self, output_filename: str = "report"):
        """
        Generates the report from the generated result graphs.

        Args:
            output_filename (str): Filename for the output report file without .pdf suffix. Default: "report"
        """
        generator = PDFOverviewGenerator()

        # prepare directory paths
        relative_input_graph_directory = self.test_run_directory / "graphs"
        relative_output_directory_path = self.test_run_directory

        # prepare file paths
        relative_input_graph_filename = (
            relative_input_graph_directory
            / LATENCIES_COMPARISON_FILENAME  # latency_comparison.png
        )

        # add elements to report pdf
        generator.setup_first_page_layout()
        generator.insert_title()
        generator.insert_box_titles()
        generator.insert_main_graph(str(relative_input_graph_filename))

        # generate and save report
        generator.save_file(
            relative_output_directory_path=relative_output_directory_path,
            output_filename=output_filename,
        )

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
            max_value=100,
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

    @staticmethod
    def __cleanup_clickhouse_database():
        clickhouse_tables = [
            "alerts",
            "batch_timestamps",
            "dns_loglines",
            "failed_dns_loglines",
            "fill_levels",
            "logline_timestamps",
            "logline_to_batches",
            "server_logs",
            "server_logs_timestamps",
            "suspicious_batch_timestamps",
            "suspicious_batches_to_batch",
        ]

        for table in clickhouse_tables:
            subprocess.run(
                [
                    "docker",
                    "exec",
                    CLICKHOUSE_CONTAINER_NAME,
                    "clickhouse-client",
                    "--query",
                    f"TRUNCATE TABLE {table};",
                ]
            ).check_returncode()

    @staticmethod
    def __check_if_all_data_is_processed(
        sleep_time: int = 30, number_of_retries: int = 1000
    ):
        sql_file = (
            BASE_DIR
            / "benchmarking"
            / "sql"
            / "entering_processed"
            / "activity_last_three_minutes.sql"
        )
        with open(sql_file) as file:
            sql_query = file.read()

        logger.info("Wait until all data has been processed...")

        for i in range(number_of_retries):
            number_of_entries = subprocess.check_output(
                [
                    "docker",
                    "exec",
                    CLICKHOUSE_CONTAINER_NAME,
                    "clickhouse-client",
                    "--query",
                    sql_query,
                ]
            )

            logger.debug(f"Check #{i}, currently: {int(number_of_entries)} entries")

            if int(number_of_entries) == 0:
                return

            time.sleep(sleep_time)

        raise RuntimeError("Maximum number of retries exceeded.")

    def __extract_all_data_from_clickhouse(self, file_identifier: str):
        self.__validate_filename(file_identifier)  # e.g. 20250709_202118_burst

        sql_directory_path = BASE_DIR / "benchmarking" / "sql"
        subdirectory_names = [
            "entering_processed",
            "latencies",
            "log_volumes",
            "full_tables",
        ]

        for subdirectory in subdirectory_names:
            sql_subdirectory_path = sql_directory_path / subdirectory

            for sql_file in sql_subdirectory_path.glob("*.sql"):
                filename = sql_file.stem
                output_filename = (
                    BASE_DIR
                    / "benchmark_results"
                    / str(file_identifier)
                    / "data"
                    / subdirectory
                    / f"{filename}.csv"
                )

                with open(sql_file) as file:
                    sql_query = file.read()

                query_output = subprocess.check_output(
                    [
                        "docker",
                        "exec",
                        "-i",
                        CLICKHOUSE_CONTAINER_NAME,
                        "clickhouse-client",
                        "--query",
                        sql_query,
                    ]
                )

                output_filename.parent.mkdir(parents=True, exist_ok=True)
                with open(output_filename, mode="wb") as file:
                    file.write(query_output)

    @staticmethod
    def __validate_filename(name: str):
        if not bool(re.match(r"^[a-zA-Z0-9_-]", name)):
            raise ValueError(
                "Invalid file name. Allowed characters are letters, numbers, '.', '_', and '-'."
            )

    def __plot_latency_comparison(self):
        """Plots the latency_comparison graph."""
        # prepare directory paths
        relative_data_path = self.test_run_directory / "data"
        relative_output_graph_directory = self.test_run_directory / "graphs"

        # create base output directory
        os.makedirs(relative_output_graph_directory, exist_ok=True)

        # prepare input file paths
        module_to_filepath = (
            MODULE_TO_CSV_FILENAME.copy()
        )  # keep original dictionary unchanged
        for module in MODULE_TO_CSV_FILENAME.keys():
            filename = MODULE_TO_CSV_FILENAME[module]
            module_to_filepath[module] = relative_data_path / "latencies" / filename

        # generate and save plots
        self.plot_generator.plot_latency(
            datafiles_to_names=module_to_filepath,
            relative_output_directory_path=relative_output_graph_directory,
            title="Latency Comparison",
            start_time=self.start_timestamp,
        )


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


class BenchmarkDatasetGenerator:
    """Generates log lines and datasets."""

    def __init__(self, data_base_path: str = "./data"):
        datasets = DatasetLoader(base_path=data_base_path, max_rows=10000)

        dataset = Dataset(
            data_path=[""],
            data=pl.concat(
                [
                    datasets.dgta_dataset.data,
                    # datasets.cic_dataset.data,
                    # datasets.bambenek_dataset.data,
                    # datasets.dga_dataset.data,
                    # datasets.dgarchive_dataset.data,
                ]
            ),
            max_rows=1000,
        )

        self.domains = dataset.data

    def generate_random_logline(
        self, statuses: list[str] = None, record_types: list[str] = None
    ):
        """Generates a (mostly) random logline."""
        if record_types is None:
            record_types = 6 * ["AAAA"] + 10 * ["A"] + ["PR", "CNAME"]

        if statuses is None:
            statuses = ["NOERROR", "NXDOMAIN"]

        # choose timestamp
        timestamp = (datetime.now() + timedelta(0, 0, random.randint(0, 900))).strftime(
            "%Y-%m-%dT%H:%M:%S.%f"
        )[:-3] + "Z"

        # choose status code
        status = random.choice(statuses)

        # choose client IP address
        number_of_subnets = 50
        client_ip = (
            f"192.168.{random.randint(0, number_of_subnets)}.{random.randint(1, 255)}"
        )

        # choose server IP address
        server_ip = f"10.10.0.{random.randint(1, 100)}"

        # choose random domain (can be malicious or benign)
        domain = self.get_random_domain()

        # choose random record type
        record_type = random.choice(record_types)

        # choose random response IP address
        def _get_random_ipv4():
            max_ipv4 = ipaddress.IPv4Address._ALL_ONES  # 2 ** 32 - 1  # noqa
            return ipaddress.IPv4Address._string_from_ip_int(  # noqa
                random.randint(0, max_ipv4)
            )

        def _get_random_ipv6():
            max_ipv6 = ipaddress.IPv6Address._ALL_ONES  # 2 ** 128 - 1  # noqa
            return ipaddress.IPv6Address._string_from_ip_int(  # noqa
                random.randint(0, max_ipv6)
            )

        ip_address_choices = [_get_random_ipv4(), _get_random_ipv6()]
        response_ip_address = random.choice(ip_address_choices)

        # choose random size
        size = f"{random.randint(50, 255)}b"

        return f"{timestamp} {status} {client_ip} {server_ip} {domain} {record_type} {response_ip_address} {size}"

    def get_random_domain(self) -> str:
        random_domain = self.domains.sample(n=1)
        return random_domain["query"].item()

    def generate_dataset(self, number_of_elements: int) -> list[str]:
        dataset = []

        for _ in range(number_of_elements):
            logline = self.generate_random_logline()
            dataset.append(logline)

        return dataset
