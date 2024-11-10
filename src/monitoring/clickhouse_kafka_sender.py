import json
import os
import sys

sys.path.append(os.getcwd())
from src.base.kafka_handler import KafkaProduceHandler


class ClickHouseKafkaSender:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.kafka_producer = KafkaProduceHandler(transactional_id="clickhouse")

    def insert(self, data: list):
        self.kafka_producer.send(
            topic=f"clickhouse_{self.table_name}",
            data=json.dumps(data),
        )
