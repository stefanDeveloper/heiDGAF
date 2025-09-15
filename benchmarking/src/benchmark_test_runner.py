import argparse
import os
import sys

sys.path.append(os.getcwd())
from benchmarking.src.test_types.burst import BurstTest
from benchmarking.src.test_types.long_term import LongTermTest
from benchmarking.src.test_types.maximum_throughput import MaximumThroughputTest
from benchmarking.src.test_types.ramp_up import RampUpTest
from src.base.log_config import get_logger
from benchmarking.src.setup_config import setup_config

logger = get_logger()
benchmark_test_config = setup_config()

test_config = benchmark_test_config["tests"]


class BenchmarkTestRunner:
    def __init__(self):
        self.parser = argparse.ArgumentParser(
            description="Execute with given test parameters. "
            "By default, configuration file values are used."
        )
        self.subparsers = self.parser.add_subparsers(
            dest="test_type",
            required=True,
            help="Available benchmark tests to execute...",
        )

        self.__add_burst_parser()
        self.__add_long_term_parser()

    def run(self):
        args = self.parser.parse_args()
        args.func(args)

    @staticmethod
    def _execute_burst_with_report(args):
        burst_test = BurstTest(
            normal_rate_interval_length=args.normal_interval_length,
            normal_rate_msg_per_sec=args.normal_data_rate,
            burst_rate_interval_length=args.burst_interval_length,
            burst_rate_msg_per_sec=args.burst_data_rate,
            number_of_repetitions=args.number_of_repetitions,
        )
        burst_test.execute_and_generate_report()

    @staticmethod
    def _execute_long_term_with_report(args):
        long_term_test = LongTermTest(
            full_length_in_minutes=args.length,
            messages_per_second=args.data_rate,
        )
        long_term_test.execute_and_generate_report()

    @staticmethod
    def _execute_maximum_throughput_with_report(args):
        maximum_throughput_test = MaximumThroughputTest(
            full_length_in_seconds=args.length,
        )
        maximum_throughput_test.execute_and_generate_report()

    @staticmethod
    def _execute_ramp_up_with_report(args):
        ramp_up_test = RampUpTest(
            messages_per_second_in_intervals=[
                int(e) for e in args.data_rates.split(",")
            ],
            interval_lengths_in_seconds=[int(e) for e in args.durations.split(",")],
        )
        ramp_up_test.execute_and_generate_report()

    def __add_burst_parser(self):
        parser = self.subparsers.add_parser("burst", help="Burst benchmark test")
        parser.add_argument(
            "--normal_data_rate",
            type=float,
            help=f"Normal Rate Test: data rate in msg/s [float | int], "
            f"default: {test_config['burst']['normal_rate']['data_rate']}",
            default=test_config["burst"]["normal_rate"]["data_rate"],
        )
        parser.add_argument(
            "--normal_interval_length",
            type=float,
            help=f"Normal Rate Test: interval length in seconds [float | int], "
            f"default: {test_config['burst']['normal_rate']['interval_length']}",
            default=test_config["burst"]["normal_rate"]["interval_length"],
        )
        parser.add_argument(
            "--burst_data_rate",
            type=float,
            help=f"Burst Rate Test: data rate in msg/s [float | int], "
            f"default: {test_config['burst']['burst_rate']['data_rate']}",
            default=test_config["burst"]["burst_rate"]["data_rate"],
        )
        parser.add_argument(
            "--burst_interval_length",
            type=float,
            help=f"Burst Rate Test: interval length in seconds [float | int], "
            f"default: {test_config['burst']['burst_rate']['interval_length']}",
            default=test_config["burst"]["burst_rate"]["interval_length"],
        )
        parser.add_argument(
            "--number_of_repetitions",
            type=int,
            help=f"number of intervals [int], default: {test_config['burst']['number_of_repetitions']}",
            default=test_config["burst"]["number_of_repetitions"],
        )
        parser.set_defaults(func=self._execute_burst_with_report)

    def __add_long_term_parser(self):
        parser = self.subparsers.add_parser(
            "long_term", help="Long-term benchmark test"
        )
        parser.add_argument(
            "--data_rate",
            type=float,
            help=f"Data rate in msg/s [float | int], default: {test_config['long_term']['data_rate']}",
            default=test_config["long_term"]["data_rate"],
        )
        parser.add_argument(
            "--length",
            type=float,
            help=f"Full length/duration in minutes [float | int], default: {test_config['long_term']['length']}",
            default=test_config["long_term"]["length"],
        )
        parser.set_defaults(func=self._execute_long_term_with_report)

    def __add_maximum_throughput_parser(self):
        parser = self.subparsers.add_parser(
            "maximum_throughput", help="Maximum-throughput benchmark test"
        )
        parser.add_argument(
            "--length",
            type=float,
            help=f"Full length/duration in minutes [float | int], default: {test_config['maximum_throughput']['length'] / 60}",
            default=test_config["maximum_throughput"]["length"] / 60,
        )
        parser.set_defaults(func=self._execute_maximum_throughput_with_report)

    def __add_ramp_up_parser(self):
        parser = self.subparsers.add_parser("ramp_up", help="Ramp-up benchmark test")
        default_data_rates = []
        default_durations = []

        for e in test_config["ramp_up"]["intervals"]:
            default_data_rates.append(e[0])
            default_durations.append(e[1])

        parser.add_argument(
            "--data_rates",
            help=f"Data rates per interval in msg/s [List[float | int], "
            f"default: {','.join(str(i) for i in default_data_rates)}",
            default=",".join(str(i) for i in default_data_rates),
        )

        parser.add_argument(
            "--durations",
            help=f"Length/duration per interval in minutes [float | int | List[float | int]], "
            f"single value applies to all intervals, "
            f"default: {','.join(str(i) for i in default_durations)}",
            default=",".join(str(i) for i in default_data_rates),
        )
        parser.set_defaults(func=self._execute_ramp_up_with_report)


if __name__ == "__main__":
    runner = BenchmarkTestRunner()
    runner.run()
