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
    """Defines the table name and allowed column fields with types."""

    name: str
    columns: dict[str, type]

    def verify(self, data: dict[str, Any]):
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
    """Manages the batches that store insert commands for each table. After the timer runs out, all batches are sent.
    If a batch reaches the maximum size, it is also sent."""

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
                    "alert_timestamp": datetime.datetime,
                    "suspicious_batch_id": uuid.UUID,
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
        """Adds the data to the batch for the table. Verifies the fields first."""
        self.tables.get(table_name).verify(data)
        self.batch.get(table_name).append(list(data.values()))

        if len(self.batch.get(table_name)) >= self.max_batch_size:
            self.insert(table_name)
        elif not self.timer:
            self._start_timer()

    def insert(self, table_name: str):
        """Inserts the batch for the given table."""
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
        """Inserts the batch for every table."""
        for e in self.batch:
            self.insert(e)

        if self.timer:
            self.timer.cancel()

        self.timer = None

    def _start_timer(self):
        if self.timer:
            self.timer.cancel()

        self.timer = Timer(BATCH_TIMEOUT, self.insert_all)
        self.timer.start()
