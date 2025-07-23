import datetime
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd

sys.path.append(os.getcwd())
from benchmarking.src.pdf_overview_generator import PDFOverviewGenerator
from benchmarking.src.plot_generator import PlotGenerator
from src.base.log_config import get_logger
from src.base.utils import setup_config
from benchmarking.src.setup_config import setup_config as setup_benchmark_test_config

logger = get_logger()
config = setup_config()
benchmark_test_config = setup_benchmark_test_config()

BASE_DIR = Path(__file__).resolve().parent.parent.parent  # heiDGAF directory

LATENCIES_COMPARISON_FILENAME = "latency_comparison.png"


class BenchmarkTestController:
    """Contains methods for running tests on remote hosts."""

    def __init__(self):
        self.test_name = None
        self.docker_container_name = None
        self.test_run_directory = None

    def run_single_test(
        self,
        test_name: str,
        docker_container_name: str = "benchmark_test_runner",
    ):
        """Sends the command to the test runner container to start the respective test with the configured
        parameters."""

        def handle_ramp_up_input() -> list[str]:
            try:
                # check type for data rates
                for i in [interval[0] for interval in test_parameters["intervals"]]:
                    float(i)

                # check type for durations
                for i in [interval[1] for interval in test_parameters["intervals"]]:
                    float(i)
            except ValueError as err:
                raise ValueError(f"Wrong argument type: {err}")

            data_rates = ",".join(
                str(i)
                for i in [interval[0] for interval in test_parameters["intervals"]]
            )
            durations = ",".join(
                str(i)
                for i in [interval[1] for interval in test_parameters["intervals"]]
            )
            return [
                f"--data_rates {data_rates}",
                f"--durations {durations}",
            ]  # arguments

        def handle_burst_input() -> list[str]:
            try:
                # check types
                normal_data_rate = float(test_parameters["normal_rate"]["data_rate"])
                normal_rate_interval_length = float(
                    test_parameters["normal_rate"]["interval_length"]
                )
                burst_data_rate = float(test_parameters["burst_rate"]["data_rate"])
                burst_rate_interval_length = float(
                    test_parameters["burst_rate"]["interval_length"]
                )
                number_of_repetitions = int(test_parameters["number_of_repetitions"])
            except ValueError as err:
                raise ValueError(f"Wrong argument type: {err}")

            return [
                f"--normal_data_rate {normal_data_rate}",
                f"--normal_interval_length {normal_rate_interval_length}",
                f"--burst_data_rate {burst_data_rate}",
                f"--burst_interval_length {burst_rate_interval_length}",
                f"--number_of_repetitions {number_of_repetitions}",
            ]  # arguments

        def handle_maximum_throughput() -> list[str]:
            try:
                # check type
                length = float(test_parameters["length"])
            except ValueError as err:
                raise ValueError(f"Wrong argument type: {err}")

            return [f"--length {length}"]  # arguments

        def handle_long_term() -> list[str]:
            try:
                # check types
                data_rate = float(test_parameters["data_rate"])
                length = float(test_parameters["length"])
            except ValueError as err:
                raise ValueError(f"Wrong argument type: {err}")

            return [
                f"--data_rate {data_rate}",
                f"--length {length}",
            ]  # arguments

        self.test_name = test_name
        self.docker_container_name = docker_container_name

        test_parameters = benchmark_test_config["tests"][test_name]

        match test_name:
            case "ramp_up":
                arguments = handle_ramp_up_input()

            case "burst":
                arguments = handle_burst_input()

            case "maximum_throughput":
                arguments = handle_maximum_throughput()

            case "long_term":
                # arguments = handle_long_term()
                arguments = [
                    "--data_rate 100 --length 2"
                ]  # TODO: Remove, only for testing

            case _:
                raise ValueError("Unknown test type")

        file_identifier, test_started_at = (
            self.__run_test_procedure_with_clickhouse_handling(arguments)
        )
        self.test_run_directory = f"{BASE_DIR}/benchmark_results/{file_identifier}"

        self._generate_plots(start_time=test_started_at)

        self._generate_report()

        self.test_name = None
        self.docker_container_name = None
        self.test_run_directory = None

    def run_configured_tests_sequentially(self):
        """Runs the tests from the configuration sequentially."""
        for test_run in benchmark_test_config["test_runs"]:
            self.run_single_test(test_run)

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
        generator.insert_main_graph(relative_input_graph_filename)

        # generate and save report
        generator.save_file(
            relative_output_directory_path=relative_output_directory_path,
            output_filename=output_filename,
        )

    def _generate_plots(self, start_time: pd.Timestamp):
        plot_generator = PlotGenerator()

        module_to_filename = {
            "Batch Handler": "batch_handler.csv",
            "Collector": "collector.csv",
            "Detector": "detector.csv",
            "Inspector": "inspector.csv",
            "Log Server": "logserver.csv",
            "Prefilter": "prefilter.csv",
        }

        def plot_latency_comparison():
            """Plots the latency_comparison graph."""
            # prepare directory paths
            relative_data_path = self.test_run_directory / "data"
            relative_output_graph_directory = self.test_run_directory / "graphs"

            # create base output directory
            os.makedirs(relative_output_graph_directory, exist_ok=True)

            # prepare input file paths
            module_to_filepath = (
                module_to_filename.copy()
            )  # keep original dictionary unchanged
            for module in module_to_filename.keys():
                filename = module_to_filename[module]
                module_to_filepath[module] = relative_data_path / "latencies" / filename

            # prepare output file paths
            relative_output_graph_filename = (
                relative_output_graph_directory / LATENCIES_COMPARISON_FILENAME
            )

            # generate and save plots
            plot_generator.plot_latency(
                datafiles_to_names=module_to_filepath,
                relative_output_directory_path=relative_output_graph_filename,
                title="Latency Comparison",
                start_time=start_time,
            )

        plot_latency_comparison()

    def __run_test_procedure_with_clickhouse_handling(
        self, arguments
    ) -> [str, pd.Timestamp]:
        def cleanup_clickhouse_database():
            subprocess.run(["sh", "benchmarking/src/cleanup.sh"]).check_returncode()

        def execute_test_return_start_time() -> pd.Timestamp:
            start_time = pd.Timestamp.utcnow().tz_localize(
                None
            )  # utc time without timezone info

            cmd = (
                f"docker exec {self.docker_container_name} "
                f"python benchmarking/src/test_types/{self.test_name}_test.py {' '.join(arguments)}"
            )
            subprocess.run(cmd, shell=True).check_returncode()

            return start_time

        def check_if_all_data_processed():
            subprocess.run(
                ["sh", "benchmarking/src/check_if_finished.sh"]
            ).check_returncode()

        def extract_all_data_from_clickhouse_return_identifier() -> str:
            identifier = (
                self.test_name + "_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            )

            subprocess.run(
                [
                    "sh",
                    "benchmarking/src/extract_data.sh",
                    f"{identifier}",  # e.g. burst_20250709_202118
                ]
            ).check_returncode()

            return identifier

        cleanup_clickhouse_database()
        logger.info(f"{self.test_name} Preparation: Database cleanup finished")

        test_started_at = execute_test_return_start_time()
        logger.info(f"{self.test_name}: Execution finished successfully")

        check_if_all_data_processed()

        file_identifier = extract_all_data_from_clickhouse_return_identifier()
        logger.info(
            f"{self.test_name}: Database entries extracted under {file_identifier}"
        )

        cleanup_clickhouse_database()
        logger.info(f"{self.test_name} Cleanup: After-test database cleanup finished")

        return file_identifier, test_started_at


if __name__ == "__main__":
    controller = BenchmarkTestController()
    controller.test_run_directory = Path("benchmark_results/long_term_20250716_195953")
    controller._generate_report()

    # controller.run_single_test("long_term")
    # controller._generate_plots(test_run_directory="benchmark_results/long_term_20250716_195953",
    #                            start_time=pd.Timestamp("2025-07-16 19:59:52"))
