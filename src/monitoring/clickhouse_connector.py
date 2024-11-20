import datetime
import os
import sys
import uuid
from abc import abstractmethod

import clickhouse_connect

sys.path.append(os.getcwd())
from src.monitoring.clickhouse_batch import ClickHouseBatchSender
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
        message_id: None | str | uuid.UUID = None,
        timestamp_in: str | datetime.datetime | None = None,
    ) -> uuid.UUID:
        # TODO: Switch to Marshmallow
        if not message_id:
            message_id = uuid.uuid4()

        if isinstance(message_id, str):
            message_id = uuid.UUID(message_id)

        if not timestamp_in:
            timestamp_in = datetime.datetime.now()

        if isinstance(timestamp_in, str):
            timestamp_in = datetime.datetime.fromisoformat(timestamp_in)

        self._add_to_batch([message_id, timestamp_in, message_text])
        return message_id


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
        message_id: str | uuid.UUID,
        event: str,
        event_timestamp: str | datetime.datetime | None = None,
    ):
        if isinstance(message_id, str):
            message_id = uuid.UUID(message_id)

        if not event_timestamp:
            event_timestamp = datetime.datetime.now()

        if isinstance(event_timestamp, str):
            event_timestamp = datetime.datetime.fromisoformat(event_timestamp)

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
        timestamp_in: str | datetime.datetime,
        timestamp_failed: str | datetime.datetime | None = None,
        reason_for_failure: str | None = None,
    ) -> None:
        if not timestamp_failed:
            timestamp_failed = datetime.datetime.now()

        if isinstance(timestamp_in, str):
            timestamp_in = datetime.datetime.fromisoformat(timestamp_in)
        if isinstance(timestamp_failed, str):
            timestamp_failed = datetime.datetime.fromisoformat(timestamp_failed)

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
        logline_id: str | uuid.UUID,
        batch_id: str | uuid.UUID,
    ):
        if isinstance(logline_id, str):
            logline_id = uuid.UUID(logline_id)
        if isinstance(batch_id, str):
            batch_id = uuid.UUID(batch_id)

        self._add_to_batch(
            [
                logline_id,
                batch_id,
            ]
        )


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
        subnet_id: str,
        timestamp: str | datetime.datetime,
        status_code: str,
        client_ip: str,
        record_type: str,
        additional_fields: str | None = None,
    ) -> uuid.UUID:
        logline_id = uuid.uuid4()

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
        return logline_id


class LoglineStatusConnector(ClickHouseConnector):
    def __init__(self):
        column_names = [
            "logline_id",
            "status",
            "exit_at_stage",
        ]

        super().__init__("logline_status", column_names)

    def insert(
        self,
        logline_id: str | uuid.UUID,
        status: str,
        exit_at_stage: str | None = None,
    ):
        if isinstance(logline_id, str):
            logline_id = uuid.UUID(logline_id)

        self._add_to_batch(
            [
                logline_id,
                status,
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
        logline_id: str | uuid.UUID,
        stage: str,
        status: str,
        timestamp: str | datetime.datetime = None,
    ) -> None:
        if isinstance(logline_id, str):
            logline_id = uuid.UUID(logline_id)

        if not timestamp:
            timestamp = datetime.datetime.now()

        if isinstance(timestamp, str):
            timestamp = datetime.datetime.fromisoformat(timestamp)

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
            "status",
            "exit_at_stage",
        ]

        super().__init__("batch_status", column_names)

    def insert(
        self,
        batch_id: str | uuid.UUID,
        status: str,
        exit_at_stage: str | None = None,
    ):
        if isinstance(batch_id, str):
            batch_id = uuid.UUID(batch_id)

        self._add_to_batch(
            [
                batch_id,
                status,
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
        batch_id: str | uuid.UUID,
        stage: str,
        status: str,
        message_count: int,
        timestamp: str | datetime.datetime = None,
    ) -> None:
        if isinstance(batch_id, str):
            batch_id = uuid.UUID(batch_id)

        if not timestamp:
            timestamp = datetime.datetime.now()

        if isinstance(timestamp, str):
            timestamp = datetime.datetime.fromisoformat(timestamp)

        self._add_to_batch(
            [
                batch_id,
                stage,
                status,
                timestamp,
                message_count,
            ]
        )
