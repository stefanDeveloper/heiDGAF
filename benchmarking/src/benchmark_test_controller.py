import os
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
                arguments = [
                    f"--normal_data_rate {test_parameters['normal_rate']['data_rate']}",
                    f"--normal_interval_length {test_parameters['normal_rate']['interval_length']}",
                    f"--burst_data_rate {test_parameters['burst_rate']['data_rate']}",
                    f"--burst_interval_length {test_parameters['burst_rate']['interval_length']}",
                    f"--number_of_repetitions {test_parameters['number_of_repetitions']}",
                ]

            case "maximum_throughput":
                arguments = [f"--length {test_parameters['length']}"]

            case "long_term":
                arguments = [
                    f"--data_rate {test_parameters['data_rate']}",
                    f"--length {test_parameters['length']}",
                ]

            case _:
                arguments = []

        if not remote_host:
            print(
                f"sudo docker exec {remote_docker_container_name} "
                f"python3 ./test_types/{test_name}_test.py {' '.join(arguments)}"
            )

        # ssh debian@129.206.4.50 \
        #   "sudo docker exec container_name python3 /script.py arg1 arg2"
        pass


if __name__ == "__main__":
    controller = BenchmarkTestController()
    controller.run_single_test("long_term")
