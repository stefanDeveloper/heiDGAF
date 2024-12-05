import datetime
import os
import sys
import uuid
from abc import abstractmethod
from typing import Optional

import clickhouse_connect

sys.path.append(os.getcwd())
from src.monitoring.clickhouse_batch_sender import ClickHouseBatchSender
from src.base.log_config import get_logger
from src.base.utils import setup_config

logger = get_logger()

CONFIG = setup_config()
CLICKHOUSE_HOSTNAME = CONFIG["environment"]["monitoring"]["clickhouse_server"][
    "hostname"
]
CREATE_TABLES_DIRECTORY = "src/monitoring/create_tables"  # TODO: Get from config


class ClickHouseConnector:
    def __init__(self, table_name: str, column_names: list[str]):
        self._table_name = table_name
        self._column_names = column_names

        self._batch_sender = ClickHouseBatchSender(
            table_name=self._table_name,
            column_names=self._column_names,
        )

    def prepare_table(self):
        def _load_contents(file_name: str) -> str:
            with open(file_name, "r") as file:
                return file.read()

        filename = self._table_name + ".sql"
        file_path = os.path.join(CREATE_TABLES_DIRECTORY, filename)
        sql_content = _load_contents(file_path)

        with clickhouse_connect.get_client(host=CLICKHOUSE_HOSTNAME) as client:
            try:
                client.command(sql_content)
            except Exception as e:
                logger.critical("Error in CREATE TABLE statement")
                raise e

    def _add_to_batch(self, data):
        self._batch_sender.add(data)

    @abstractmethod
    def insert(self, *args, **kwargs):
        pass


class ServerLogsConnector(ClickHouseConnector):
    def __init__(self):
        column_names = [
            "message_id",
            "timestamp_in",
            "message_text",
        ]

        super().__init__("server_logs", column_names)

    def insert(
        self,
        message_text: str,
        message_id: uuid.UUID,
        timestamp_in: datetime.datetime,
    ):
        self._add_to_batch([message_id, timestamp_in, message_text])


class ServerLogsTimestampsConnector(ClickHouseConnector):
    def __init__(self):
        column_names = [
            "message_id",
            "event",
            "event_timestamp",
        ]

        super().__init__("server_logs_timestamps", column_names)

    def insert(
        self,
        message_id: uuid.UUID,
        event: str,
        event_timestamp: datetime.datetime,
    ):
        self._add_to_batch([message_id, event, event_timestamp])


class FailedDNSLoglinesConnector(ClickHouseConnector):
    def __init__(self):
        column_names = [
            "message_text",
            "timestamp_in",
            "timestamp_failed",
            "reason_for_failure",
        ]

        super().__init__("failed_dns_loglines", column_names)

    def insert(
        self,
        message_text: str,
        timestamp_in: datetime.datetime,
        timestamp_failed: datetime.datetime,
        reason_for_failure: Optional[str] = None,
    ) -> None:
        self._add_to_batch(
            [message_text, timestamp_in, timestamp_failed, reason_for_failure]
        )


class LoglineToBatchesConnector(ClickHouseConnector):
    def __init__(self):
        column_names = [
            "logline_id",
            "batch_id",
        ]

        super().__init__("logline_to_batches", column_names)

    def insert(
        self,
        logline_id: uuid.UUID,
        batch_id: uuid.UUID,
    ):
        self._add_to_batch([logline_id, batch_id])


class DNSLoglinesConnector(ClickHouseConnector):
    def __init__(self):
        column_names = [
            "logline_id",
            "subnet_id",
            "timestamp",
            "status_code",
            "client_ip",
            "record_type",
            "additional_fields",
        ]

        super().__init__("dns_loglines", column_names)

    def insert(
        self,
        logline_id: str | uuid.UUID,
        subnet_id: str,
        timestamp: str | datetime.datetime,
        status_code: str,
        client_ip: str,
        record_type: str,
        additional_fields: Optional[str] = None,
    ):
        self._add_to_batch(
            [
                logline_id,
                subnet_id,
                timestamp,
                status_code,
                client_ip,
                record_type,
                additional_fields,
            ]
        )


class LoglineStatusConnector(ClickHouseConnector):
    def __init__(self):
        column_names = [
            "logline_id",
            "is_active",
            "exit_at_stage",
        ]

        super().__init__("logline_status", column_names)

    def insert(
        self,
        logline_id: uuid.UUID,
        is_active: bool,
        exit_at_stage: Optional[str] = None,
    ):
        self._add_to_batch(
            [
                logline_id,
                is_active,
                exit_at_stage,
            ]
        )


class LoglineTimestampsConnector(ClickHouseConnector):
    def __init__(self):
        column_names = [
            "logline_id",
            "stage",
            "status",
            "timestamp",
        ]

        super().__init__("logline_timestamps", column_names)

    def insert(
        self,
        logline_id: uuid.UUID,
        stage: str,
        status: str,
        timestamp: datetime.datetime,
    ) -> None:
        self._add_to_batch(
            [
                logline_id,
                stage,
                status,
                timestamp,
            ]
        )


class BatchStatusConnector(ClickHouseConnector):
    def __init__(self):
        column_names = [
            "batch_id",
            "is_active",
            "exit_at_stage",
        ]

        super().__init__("batch_status", column_names)

    def insert(
        self,
        batch_id: uuid.UUID,
        is_active: bool,
        exit_at_stage: Optional[str] = None,
    ):
        self._add_to_batch(
            [
                batch_id,
                is_active,
                exit_at_stage,
            ]
        )


class BatchTimestampsConnector(ClickHouseConnector):
    def __init__(self):
        column_names = [
            "batch_id",
            "stage",
            "status",
            "timestamp",
            "message_count",
        ]

        super().__init__("batch_timestamps", column_names)

    def insert(
        self,
        batch_id: uuid.UUID,
        stage: str,
        status: str,
        message_count: int,
        timestamp: datetime.datetime,
    ) -> None:
        self._add_to_batch(
            [
                batch_id,
                stage,
                status,
                timestamp,
                message_count,
            ]
        )
