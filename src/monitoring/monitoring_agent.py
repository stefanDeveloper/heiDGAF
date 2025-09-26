import asyncio
import os
import sys
from dataclasses import asdict

import marshmallow_dataclass

sys.path.append(os.getcwd())
from src.monitoring.clickhouse_batch_sender import *
from src.base.kafka_handler import SimpleKafkaConsumeHandler
from src.base.data_classes.clickhouse_connectors import TABLE_NAME_TO_TYPE
from src.base.log_config import get_logger
from src.base.utils import setup_config

logger = get_logger()

CONFIG = setup_config()
CREATE_TABLES_DIRECTORY = "docker/create_tables"  # TODO: Get from config
CLICKHOUSE_HOSTNAME = CONFIG["environment"]["monitoring"]["clickhouse_server"][
    "hostname"
]


def prepare_all_tables():
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


class MonitoringAgent:
    def __init__(self):
        self.table_names = [
            "server_logs",
            "server_logs_timestamps",
            "failed_loglines",
            "logline_to_batches",
            "loglines",
            "logline_timestamps",
            "batch_timestamps",
            "suspicious_batches_to_batch",
            "suspicious_batch_timestamps",
            "alerts",
            "fill_levels",
            "batch_tree",
        ]

        self.topics = [f"clickhouse_{table_name}" for table_name in self.table_names]
        self.kafka_consumer = SimpleKafkaConsumeHandler(self.topics)
        self.batch_sender = ClickHouseBatchSender()

    async def start(self):
        loop = asyncio.get_running_loop()

        while True:
            try:
                key, value, topic = await loop.run_in_executor(
                    None, self.kafka_consumer.consume
                )
                logger.debug(f"From Kafka: {value}")

                table_name = topic.replace("clickhouse_", "")
                data_schema = marshmallow_dataclass.class_schema(
                    TABLE_NAME_TO_TYPE.get(table_name)
                )()
                data = data_schema.loads(value)

                self.batch_sender.add(table_name, asdict(data))
            except KeyboardInterrupt:
                logger.info("Stopped MonitoringAgent.")
                break
            except Exception as e:
                logger.warning(e)


def main():
    clickhouse_consumer = MonitoringAgent()
    asyncio.run(clickhouse_consumer.start())


if __name__ == "__main__":  # pragma: no cover
    main()
