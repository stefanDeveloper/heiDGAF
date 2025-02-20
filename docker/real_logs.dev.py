import os
import sys
import time

import numpy as np
import polars as pl
from confluent_kafka import KafkaError

sys.path.append(os.getcwd())
from src.base.kafka_handler import SimpleKafkaProduceHandler
from src.mock.log_generator import generate_dns_log_line
from src.base.log_config import get_logger
from src.train.dataset import Dataset, DatasetLoader

logger = get_logger()
kafka_producer = SimpleKafkaProduceHandler()

if __name__ == "__main__":
    try:
        data_base_path: str = "./data"
        datasets = DatasetLoader(base_path=data_base_path, max_rows=10000)
        dataset = Dataset(
            data_path="",
            data=pl.concat(
                [
                    datasets.dgta_dataset.data,
                    # datasets.cic_dataset.data,
                    # datasets.bambenek_dataset.data,
                    # datasets.dga_dataset.data,
                    # datasets.dgarchive_dataset.data,
                ]
            ),
            max_rows=100,
        )
        data = dataset.data
        print(data)
        np.random.seed(None)
        while True:
            for i in range(0, 10):
                random_domain = data.sample(n=1)
                logline = generate_dns_log_line(random_domain["query"].item())
                try:
                    kafka_producer.produce(
                        "pipeline-logserver_in", logline.encode("utf-8")
                    )
                    logger.info(f"Sent logline: {logline}")
                except KafkaError:
                    logger.warning(KafkaError)
            time.sleep(0.1)
    except KeyboardInterrupt:
        pass
