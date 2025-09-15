import os
import re
import subprocess
import sys

sys.path.append(os.getcwd())
from src.base.log_config import get_logger
from src.base.utils import setup_config
from benchmarking.src.setup_config import setup_config as setup_benchmark_config

logger = get_logger()
config = setup_config()
benchmark_test_config = setup_benchmark_config()


class BenchmarkController:
    """Contains methods for running tests on remote hosts."""

    def __init__(self):
        self.test_parameters = None

    def run_single_test(
        self,
        test_name: str,
        docker_container_name: str = "benchmark_test_runner",
    ):
        """Sends the command to the test runner container to start the respective test with the configured
        parameters."""
        self.test_parameters = benchmark_test_config["tests"][test_name]

        match test_name:  # acts as whitelist
            case "ramp_up":
                arguments = self.__handle_ramp_up_input()

            case "burst":
                arguments = self.__handle_burst_input()

            case "maximum_throughput":
                arguments = self.__handle_maximum_throughput()

            case "long_term":
                arguments = self.__handle_long_term()

            case _:
                raise ValueError("Unknown test type")

        docker_container_name = str(docker_container_name)
        self.__validate_name(docker_container_name)

        cmd = [
            # "docker",  # TODO: Use SSH
            # "exec",
            # docker_container_name,
            "python",
            f"benchmarking/src/test_types/{test_name}.py",
            *arguments,
        ]
        subprocess.run(cmd).check_returncode()

        self.test_parameters = None

    def run_configured_tests_sequentially(self):
        """Runs the tests from the configuration sequentially."""
        for test_run in benchmark_test_config["test_runs"]:
            self.run_single_test(test_run)

    def __handle_ramp_up_input(self) -> list[str]:
        try:
            # check type for data rates
            for i in [interval[0] for interval in self.test_parameters["intervals"]]:
                float(i)

            # check type for durations
            for i in [interval[1] for interval in self.test_parameters["intervals"]]:
                float(i)
        except ValueError as err:
            raise ValueError(f"Wrong argument type: {err}")

        data_rates = ",".join(
            str(i)
            for i in [interval[0] for interval in self.test_parameters["intervals"]]
        )
        durations = ",".join(
            str(i)
            for i in [interval[1] for interval in self.test_parameters["intervals"]]
        )
        return [
            "--data_rates",
            data_rates,
            "--durations",
            durations,
        ]  # arguments

    def __handle_burst_input(self) -> list[str]:
        try:
            # check types
            normal_data_rate = float(self.test_parameters["normal_rate"]["data_rate"])
            normal_rate_interval_length = float(
                self.test_parameters["normal_rate"]["interval_length"]
            )
            burst_data_rate = float(self.test_parameters["burst_rate"]["data_rate"])
            burst_rate_interval_length = float(
                self.test_parameters["burst_rate"]["interval_length"]
            )
            number_of_repetitions = int(self.test_parameters["number_of_repetitions"])
        except ValueError as err:
            raise ValueError(f"Wrong argument type: {err}")

        return [
            "--normal_data_rate",
            str(normal_data_rate),
            "--normal_interval_length",
            str(normal_rate_interval_length),
            "--burst_data_rate",
            str(burst_data_rate),
            "--burst_interval_length",
            str(burst_rate_interval_length),
            "--number_of_repetitions",
            str(number_of_repetitions),
        ]  # arguments

    def __handle_maximum_throughput(self) -> list[str]:
        try:
            # check type
            length = float(self.test_parameters["length"])
        except ValueError as err:
            raise ValueError(f"Wrong argument type: {err}")

        return ["--length", str(length)]  # arguments

    def __handle_long_term(self) -> list[str]:
        try:
            # check types
            data_rate = float(self.test_parameters["data_rate"])
            length = float(self.test_parameters["length"])
        except ValueError as err:
            raise ValueError(f"Wrong argument type: {err}")

        return [
            "--data_rate",
            str(data_rate),
            "--length",
            str(length),
        ]  # arguments

    @staticmethod
    def __validate_name(name: str):
        if not bool(re.match(r"[a-zA-Z0-9_]", name)):
            raise ValueError(
                "Test Name can only include alphabetic letters, numbers, and underscores"
            )


if __name__ == "__main__":
    controller = BenchmarkController()
    controller.run_configured_tests_sequentially()
