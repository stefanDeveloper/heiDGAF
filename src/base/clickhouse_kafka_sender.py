"""
The ClickHouseKafkaSender serves as the sender for all inserts into ClickHouse. Whenever a class wants to insert
into a ClickHouse table, the ClickHouseKafkaSender is used to send the respective insert via Kafka.
"""

import os
import sys

import marshmallow_dataclass

sys.path.append(os.getcwd())
from src.base.data_classes.clickhouse_connectors import TABLE_NAME_TO_TYPE
from src.base.kafka_handler import SimpleKafkaProduceHandler
from src.base.log_config import get_logger

logger = get_logger()


class ClickHouseKafkaSender:
    """Sends insert operations for the specified table via Kafka to the MonitoringAgent."""

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.kafka_producer = SimpleKafkaProduceHandler()
        self.data_schema = marshmallow_dataclass.class_schema(
            TABLE_NAME_TO_TYPE.get(table_name)
        )()

    def insert(self, data: dict):
        """Produces the insert operation to Kafka."""
        self.kafka_producer.produce(
            topic=f"clickhouse_{self.table_name}",
            data=self.data_schema.dumps(data),
        )
