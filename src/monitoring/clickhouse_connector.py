import datetime
import os
import sys
import uuid

import clickhouse_connect

sys.path.append(os.getcwd())
from src.monitoring.clickhouse_batch import ClickHouseBatchSender
from src.base.kafka_handler import KafkaConsumeHandler
from src.base.log_config import get_logger
from src.base.utils import setup_config

logger = get_logger()

CONFIG = setup_config()
CLICKHOUSE_HOSTNAME = CONFIG["environment"]["monitoring"]["clickhouse_server"][
    "hostname"
]
CREATE_TABLES_DIRECTORY = "create_tables"  # TODO: Get from config


class ClickHouseConnector:
    def __init__(self, table_name: str, column_names: list[str]):
        self._table_name = table_name
        self._column_names = column_names
        self._topic = f"clickhouse_{table_name}"

        self._kafka_consumer = KafkaConsumeHandler(self._topic)
        self._batch_sender = ClickHouseBatchSender(
            kafka_topic=self._topic,
            table_name=self._table_name,
            column_names=self._column_names,
        )

    def prepare_tables(self):
        def _load_contents(file_name: str) -> str:
            with open(file_name, "r") as file:
                return file.read()

        for filename in os.listdir(CREATE_TABLES_DIRECTORY):
            if filename.endswith(".sql"):
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


class ServerLogsConnector(ClickHouseConnector):
    def __init__(self):
        column_names = [
            "message_id",
            "timestamp_in",
            "message_text",
        ]

        super().__init__("server_logs", column_names)

    def insert(
        self, message_text: str, timestamp_in: datetime.datetime | None = None
    ) -> uuid.UUID:
        if not timestamp_in:
            timestamp_in = datetime.datetime.now()
        timestamp_in = timestamp_in.strftime("%Y-%m-%d %H:%M:%S.%f")

        message_id = uuid.uuid4()
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
        message_id: uuid.UUID,
        event: str,
        event_timestamp: datetime.datetime | None = None,
    ) -> uuid.UUID:
        if not event_timestamp:
            event_timestamp = datetime.datetime.now()
        event_timestamp = event_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")

        self._add_to_batch([message_id, event, event_timestamp])
        return message_id


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
        timestamp_failed: datetime.datetime | None = None,
        reason_for_failure: str | None = None,
    ) -> None:
        if not timestamp_failed:
            timestamp_failed = datetime.datetime.now()

        timestamp_in = timestamp_in.strftime("%Y-%m-%d %H:%M:%S.%f")
        timestamp_failed = timestamp_failed.strftime("%Y-%m-%d %H:%M:%S.%f")

        self._add_to_batch(
            [message_text, timestamp_in, timestamp_failed, reason_for_failure]
        )
