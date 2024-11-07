import os
import sys
from threading import Timer

import clickhouse_connect

sys.path.append(os.getcwd())
from src.base.log_config import get_logger
from src.base.utils import setup_config

logger = get_logger()

CONFIG = setup_config()
CLICKHOUSE_HOSTNAME = CONFIG["environment"]["monitoring"]["clickhouse_server"][
    "hostname"
]
BATCH_SIZE = CONFIG["pipeline"]["monitoring"]["clickhouse_connector"]["batch_size"]
BATCH_TIMEOUT = CONFIG["pipeline"]["monitoring"]["clickhouse_connector"][
    "batch_timeout"
]


class ClickHouseBatchSender:
    def __init__(self, kafka_topic: str, table_name: str, column_names: list[str]):
        self.kafka_topic = kafka_topic
        self.table_name = table_name
        self.column_names = column_names

        self.max_batch_size = BATCH_SIZE
        self.batch_timeout = BATCH_TIMEOUT

        self.timer = None
        self.batch = []
        self._client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOSTNAME,
        )

    def __del__(self):
        self.insert_all()

    def add(self, data: list[str] | list[list[str]]):
        def _add_element(element):
            if len(element) != len(self.column_names):
                raise ValueError(
                    "Number of elements in the insert does not match the number of columns"
                )

            self.batch.append(element)

        if any(isinstance(e, list) for e in data):
            for e in data:
                _add_element(e)
        else:
            _add_element(data)

        if len(self.batch) >= self.max_batch_size:
            self.insert_all()
        elif not self.timer:
            self._start_timer()

    def insert_all(self):
        if self.batch:
            self._client.insert(
                self.table_name,
                self.batch,
                self.column_names,
            )
            logger.info(
                f"""
                self.client.insert(
                    {self.table_name=},
                    {self.batch=},
                    {self.column_names=},
                )
            """
            )
            self.batch = []

        if self.timer:
            self.timer.cancel()

    def _start_timer(self):
        if self.timer:
            self.timer.cancel()

        self.timer = Timer(BATCH_TIMEOUT, self.insert_all)
        self.timer.start()
