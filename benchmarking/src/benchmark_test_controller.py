import os
import subprocess
import sys

sys.path.append(os.getcwd())
from src.base.log_config import get_logger
from src.base.utils import setup_config
from benchmarking.src.setup_config import setup_config as setup_benchmark_test_config

logger = get_logger()
config = setup_config()
benchmark_test_config = setup_benchmark_test_config()


class BenchmarkTestController:
    """Contains methods for running tests on remote hosts."""

    def run_single_test(
        self,
        test_name: str,
        remote_host=None,
        remote_docker_container_name: str = "benchmark_test_runner",
    ):
        """Sends the command to the remote host to start the respective test with the configured parameters."""
        test_parameters = benchmark_test_config["tests"][test_name]

        match test_name:
            case "ramp_up":
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
                arguments = [f"--data_rates {data_rates}", f"--durations {durations}"]

            case "burst":
                try:
                    # check types
                    normal_data_rate_arg = float(
                        test_parameters["normal_rate"]["data_rate"]
                    )
                    normal_rate_interval_length_arg = float(
                        test_parameters["normal_rate"]["interval_length"]
                    )
                    burst_data_rate_arg = float(
                        test_parameters["burst_rate"]["data_rate"]
                    )
                    burst_rate_interval_length_arg = float(
                        test_parameters["burst_rate"]["interval_length"]
                    )
                    number_of_repetitions_arg = int(
                        test_parameters["number_of_repetitions"]
                    )
                except ValueError as err:
                    raise ValueError(f"Wrong argument type: {err}")

                arguments = [
                    f"--normal_data_rate {normal_data_rate_arg}",
                    f"--normal_interval_length {normal_rate_interval_length_arg}",
                    f"--burst_data_rate {burst_data_rate_arg}",
                    f"--burst_interval_length {burst_rate_interval_length_arg}",
                    f"--number_of_repetitions {number_of_repetitions_arg}",
                ]

            case "maximum_throughput":
                try:
                    # check type
                    length_arg = float(test_parameters["length"])
                except ValueError as err:
                    raise ValueError(f"Wrong argument type: {err}")

                arguments = [f"--length {length_arg}"]

                # ONLY FOR TESTING, TODO
                arguments = [f"--length 1"]

            case "long_term":
                try:
                    # check types
                    data_rate_arg = float(test_parameters["data_rate"])
                    length_arg = float(test_parameters["length"])
                except ValueError as err:
                    raise ValueError(f"Wrong argument type: {err}")

                arguments = [
                    f"--data_rate {data_rate_arg}",
                    f"--length {length_arg}",
                ]

            case _:
                arguments = []

        if not remote_host:
            cmd = (
                f"docker exec {remote_docker_container_name} "
                f"python benchmarking/src/test_types/{test_name}_test.py {' '.join(arguments)}"
            )
            subprocess.run(cmd, shell=True)

            logger.info("READY")

            # os.system("sh benchmarking/src/check_if_finished.sh")

        # ssh debian@129.206.4.50 \
        #   "sudo docker exec container_name python3 /script.py arg1 arg2"
        pass


if __name__ == "__main__":
    controller = BenchmarkTestController()
    controller.run_single_test("maximum_throughput")
