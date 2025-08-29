import os
import sys

sys.path.append(os.getcwd())
from benchmarking.src.test_types.base import BaseTest


class BenchmarkTestRunner:
    pass


if __name__ == "__main__":
    # sut = BaseTest("TestName", 1234)
    BaseTest._BaseTest__cleanup_clickhouse_database()  # noqa
