import json
import os
import sys

sys.path.append(os.getcwd())
from src.base.kafka_handler import SimpleKafkaProduceHandler
from src.base.log_config import get_logger

logger = get_logger()


class ClickHouseKafkaSender:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.kafka_producer = SimpleKafkaProduceHandler()

    def insert(self, data: dict):
        self.kafka_producer.produce(
            topic=f"clickhouse_{self.table_name}",
            data=json.dumps(data, default=str),
        )
