import datetime
import os
import sys
import uuid
from dataclasses import dataclass
from threading import Timer, Lock
from typing import Any, Optional, get_origin

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


@dataclass
class Table:
    """Defines the table name and allowed column fields with types.

    Stores metadata about ClickHouse table structure including column names
    and their expected data types for validation during batch insertion.
    """

    name: str
    columns: dict[str, type]

    def verify(self, data: dict[str, Any]):
        """Verify if the data has the correct columns and types.

        Validates that the provided data dictionary contains the expected columns
        with correct data types according to the table schema definition.

        Args:
            data (dict): The values for each cell.

        Raises:
            ValueError: If column count or column names don't match expected schema.
            TypeError: If data types don't match expected column types.
        """
        if len(data) != len(self.columns):
            raise ValueError(
                f"Wrong number of fields in data: Expected {len(self.columns)}, got {len(data)}"
            )

        for e in data:
            if e not in self.columns:
                raise ValueError(f"Wrong column name: Expected one of {self.columns}")

            expected_type = self.columns.get(e)
            value = data.get(e)

            if value is not None and not isinstance(value, expected_type):
                origin = get_origin(expected_type)
                if origin is not None and not isinstance(value, origin):
                    raise TypeError(
                        f"Column '{e}' expected type {expected_type}, got {type(value)}"
                    )
                elif origin is None:
                    raise TypeError(
                        f"Column '{e}' expected type {expected_type}, got {type(value)}"
                    )


class ClickHouseBatchSender:
    """Manages batched insert operations for ClickHouse tables.

    Collects insert commands in batches and sends them to ClickHouse when either
    the batch size limit is reached or a timeout occurs. Provides efficient bulk
    insertion with automatic schema validation for all monitored tables.
    """

    def __init__(self):
        self.tables = {
            "server_logs": Table(
                "server_logs",
                {
                    "message_id": uuid.UUID,
                    "timestamp_in": datetime.datetime,
                    "message_text": str,
                },
            ),
            "server_logs_timestamps": Table(
                "server_logs_timestamps",
                {
                    "message_id": uuid.UUID,
                    "event": str,
                    "event_timestamp": datetime.datetime,
                },
            ),
            "failed_dns_loglines": Table(
                "failed_dns_loglines",
                {
                    "message_text": str,
                    "timestamp_in": datetime.datetime,
                    "timestamp_failed": datetime.datetime,
                    "reason_for_failure": Optional[str],
                },
            ),
            "logline_to_batches": Table(
                "logline_to_batches",
                {
                    "logline_id": uuid.UUID,
                    "batch_id": uuid.UUID,
                },
            ),
            "dns_loglines": Table(
                "dns_loglines",
                {
                    "logline_id": uuid.UUID,
                    "subnet_id": str,
                    "timestamp": datetime.datetime,
                    "status_code": str,
                    "client_ip": str,
                    "record_type": str,
                    "additional_fields": Optional[str],
                },
            ),
            "logline_timestamps": Table(
                "logline_timestamps",
                {
                    "logline_id": uuid.UUID,
                    "stage": str,
                    "status": str,
                    "timestamp": datetime.datetime,
                    "is_active": bool,
                },
            ),
            "batch_timestamps": Table(
                "batch_timestamps",
                {
                    "batch_id": uuid.UUID,
                    "stage": str,
                    "status": str,
                    "timestamp": datetime.datetime,
                    "is_active": bool,
                    "message_count": int,
                },
            ),
            "suspicious_batches_to_batch": Table(
                "suspicious_batches_to_batch",
                {
                    "suspicious_batch_id": uuid.UUID,
                    "batch_id": uuid.UUID,
                },
            ),
            "suspicious_batch_timestamps": Table(
                "suspicious_batch_timestamps",
                {
                    "suspicious_batch_id": uuid.UUID,
                    "client_ip": str,
                    "stage": str,
                    "status": str,
                    "timestamp": datetime.datetime,
                    "is_active": bool,
                    "message_count": int,
                },
            ),
            "alerts": Table(
                "alerts",
                {
                    "client_ip": str,
                    "suspicious_batch_id": uuid.UUID,
                    "alert_timestamp": datetime.datetime,
                    "overall_score": float,
                    "domain_names": str,
                    "result": str,
                },
            ),
            "fill_levels": Table(
                "fill_levels",
                {
                    "timestamp": datetime.datetime,
                    "stage": str,
                    "entry_type": str,
                    "entry_count": int,
                },
            ),
        }

        self.max_batch_size = BATCH_SIZE
        self.batch_timeout = BATCH_TIMEOUT

        self.timer = None
        self.batch = {key: [] for key in self.tables}
        self._client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOSTNAME,
        )
        self.lock = Lock()

    def __del__(self):
        self.insert_all()

    def add(self, table_name: str, data: dict[str, Any]):
        """Adds the data to the batch for the table.

        Verifies the data fields first, then adds the data to the appropriate
        table batch. Triggers immediate insertion if batch size limit is reached.

        Args:
            table_name (str): Name of the table to add data to.
            data (dict): The values for each cell in the table.

        Raises:
            ValueError: If table name is invalid or data format is incorrect.
            TypeError: If data types don't match table schema.
        """
        self.tables.get(table_name).verify(data)
        self.batch.get(table_name).append(list(data.values()))

        if len(self.batch.get(table_name)) >= self.max_batch_size:
            self.insert(table_name)

        if not self.timer:
            self._start_timer()

    def insert(self, table_name: str):
        """Inserts the batch for the given table.

        Executes the accumulated batch insert operation for the specified table
        and clears the batch after successful insertion.

        Args:
            table_name (str): Name of the table to insert data to.
        """
        if self.batch[table_name]:
            with self.lock:
                self._client.insert(
                    table_name,
                    self.batch.get(table_name),
                    column_names=list(self.tables.get(table_name).columns),
                )
                logger.debug(
                    f"Inserted {table_name=},{self.batch.get(table_name)=},{list(self.tables.get(table_name).columns)=}"
                )
                self.batch[table_name] = []

    def insert_all(self):
        """Inserts the batch for every table.

        Executes batch insert operations for all tables with pending data
        and cancels the current timer if active.
        """
        for table in self.batch:
            self.insert(table)

        if self.timer:
            self.timer.cancel()

        self.timer = None

    def _start_timer(self):
        """Set the timer for batch processing of data insertion.

        Cancels any existing timer and starts a new one that will trigger
        batch insertion after the configured timeout period.
        """
        if self.timer:
            self.timer.cancel()

        self.timer = Timer(BATCH_TIMEOUT, self.insert_all)
        self.timer.start()
