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

sys.path.append(os.getcwd())
from src.base.kafka_handler import SimpleKafkaProduceHandler
from benchmarking.test_runner.plot_generator import PlotGenerator
from src.base.utils import setup_config
from benchmarking.test_runner.pdf_overview_generator import PDFOverviewGenerator
from src.train.dataset import Dataset, DatasetLoader
from src.base.log_config import get_logger

logger = get_logger()
config = setup_config()

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent  # heiDGAF directory
PRODUCE_TO_TOPIC: str = config["environment"]["kafka_topics"]["pipeline"][
    "logserver_in"
]
LATENCIES_COMPARISON_FILENAME: str = "latency_comparison.png"
MODULE_TO_CSV_FILENAME: dict[str, str] = {
    "Batch Handler": "batch_handler.csv",
    "Collector": "collector.csv",
    "Detector": "detector.csv",
    "Inspector": "inspector.csv",
    "Log Server": "logserver.csv",
    "Prefilter": "prefilter.csv",
}
CLICKHOUSE_CONTAINER_NAME: str = config["environment"]["monitoring"][
    "clickhouse_server"
]["hostname"]


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
        """Handles the entire benchmark test procedure required for generating a report. Executes the test and
        generates the report from the resulting data."""
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
        """Generates all available plots from the obtained data."""
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
        """Clears the entire ClickHouse database by emptying all benchmark-related tables."""
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
        sleep_time_in_seconds: int = 30, number_of_retries: int = 1000
    ):
        """
        Blocks until all data currently handled by the pipeline is processed, i.e. there have been no new database
        entries in the last 3 minutes.

        Args:
            sleep_time_in_seconds (int): Time to sleep between checks in seconds; default: 30
            number_of_retries (int): Number of total checks, upper bound to prevent infinite loop; default: 1000
        """
        sql_file = (
            BASE_DIR
            / "benchmarking"
            / "sql_queries"
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

            time.sleep(sleep_time_in_seconds)

        raise RuntimeError("Maximum number of retries exceeded.")

    def __extract_all_data_from_clickhouse(self, file_identifier: str):
        """
        Extracts every benchmark-related table and more specific queries from ClickHouse and stores the data as
        .csv files under the benchmark_results directory.

        Args:
            file_identifier (str): Identifying name of this specific test execution, e.g. 20250709_202118_burst
        """
        self.__validate_filename(file_identifier)  # e.g. 20250709_202118_burst

        sql_directory_path = BASE_DIR / "benchmarking" / "sql_queries"
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
        """Plots the latency comparison graph."""
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
            module_to_filepath[module] = str(
                relative_data_path / "latencies" / filename
            )

        # generate and save plots
        self.plot_generator.plot_latency(
            datafiles_to_names=module_to_filepath,
            relative_output_directory_path=relative_output_graph_directory,
            title="Latency Comparison",
            start_time=self.start_timestamp,
        )


class BenchmarkDatasetGenerator:
    """Generates log lines and datasets."""

    def __init__(self, data_base_path: str = "./data"):
        datasets = DatasetLoader(base_path=data_base_path, max_rows=10000)

        dataset = Dataset(
            data_path="",
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
