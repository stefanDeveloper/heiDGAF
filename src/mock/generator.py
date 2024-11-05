import os
import socket
import sys
import time
import polars as pl
import numpy as np


sys.path.append(os.getcwd())
from src.mock.log_generator import generate_dns_log_line
from src.base.log_config import get_logger
from src.train.dataset import Dataset, DatasetLoader

logger = get_logger()

if __name__ == "__main__":
    data_base_path: str = "./data"
    datasets = DatasetLoader(base_path=data_base_path, max_rows=10000)
    dataset = Dataset(
        data_path="",
        data=pl.concat(
            [
                datasets.dgta_dataset.data,
                datasets.cic_dataset.data,
                datasets.bambenek_dataset.data,
                datasets.dga_dataset.data,
                datasets.dgarchive_dataset.data,
            ]
        ),
        max_rows=100,
    )
    data = dataset.data
    print(data)
    np.random.seed(None)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((str("127.0.0.1"), 9998))
        while True:
            for i in range(0, 10):
                random_domain = data.sample(n=1)
                logline = generate_dns_log_line(random_domain["query"].item())
                client_socket.send(logline.encode("utf-8"))
                logger.info(f"Sent logline: {logline}")
            time.sleep(0.1)
