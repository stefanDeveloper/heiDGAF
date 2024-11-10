import asyncio
import json
import os
import sys

sys.path.append(os.getcwd())
from src.monitoring.clickhouse_connector import *
from src.base.kafka_handler import KafkaConsumeHandler
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
            "logline_status": LoglineStatusConnector(),
            "logline_timestamps": LoglineTimestampsConnector(),
            "batch_status": BatchStatusConnector(),
            "batch_timestamps": BatchTimestampsConnector(),
        }

        self.topics = [f"clickhouse_{table_name}" for table_name in self.connectors]
        self.kafka_consumer = KafkaConsumeHandler(self.topics)

    async def start(self):
        prepare_all_tables()
        loop = asyncio.get_running_loop()

        try:
            while True:
                key, value, topic = await loop.run_in_executor(
                    None, self.kafka_consumer.consume
                )
                logger.info(f"Received message via Kafka:\n    ⤷  {value}")

                data = json.loads(value)
                task = self.connectors[value].insert(**data)
                await task
        except KeyboardInterrupt:
            logger.info("Stop consuming...")


if __name__ == "__main__":
    logger.info("Starting Monitoring Agent...")
    clickhouse_consumer = MonitoringAgent()
    asyncio.run(clickhouse_consumer.start())
