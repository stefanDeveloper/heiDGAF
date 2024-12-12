import asyncio
import os
import sys
from dataclasses import asdict

import marshmallow_dataclass

sys.path.append(os.getcwd())
from src.monitoring.clickhouse_connector import *
from src.base.kafka_handler import SimpleKafkaConsumeHandler
from src.base.data_classes.clickhouse_connectors import TABLE_NAME_TO_TYPE
from src.base.log_config import get_logger
from src.base.utils import setup_config

logger = get_logger()

CONFIG = setup_config()
CREATE_TABLES_DIRECTORY = "src/monitoring/create_tables"  # TODO: Get from config


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
        self.connectors = {
            "server_logs": ServerLogsConnector(),
            "server_logs_timestamps": ServerLogsTimestampsConnector(),
            "failed_dns_loglines": FailedDNSLoglinesConnector(),
            "logline_to_batches": LoglineToBatchesConnector(),
            "dns_loglines": DNSLoglinesConnector(),
            "logline_timestamps": LoglineTimestampsConnector(),
            "batch_timestamps": BatchTimestampsConnector(),
        }

        self.topics = [f"clickhouse_{table_name}" for table_name in self.connectors]
        self.kafka_consumer = SimpleKafkaConsumeHandler(self.topics)

    async def start(self):
        loop = asyncio.get_running_loop()

        try:
            while True:
                key, value, topic = await loop.run_in_executor(
                    None, self.kafka_consumer.consume
                )
                logger.debug(f"From Kafka: {value}")

                table_name = topic.replace("clickhouse_", "")
                data_schema = marshmallow_dataclass.class_schema(
                    TABLE_NAME_TO_TYPE.get(table_name)
                )()
                data = data_schema.loads(value)

                self.connectors[table_name].insert(**asdict(data))
        except KeyboardInterrupt:
            logger.info("Stopped MonitoringAgent.")


def main():
    prepare_all_tables()
    clickhouse_consumer = MonitoringAgent()
    asyncio.run(clickhouse_consumer.start())


if __name__ == "__main__":  # pragma: no cover
    main()
