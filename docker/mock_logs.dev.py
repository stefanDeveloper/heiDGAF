import os
import random
import sys
import time

sys.path.append(os.getcwd())
from src.base.kafka_handler import SimpleKafkaProduceHandler
from src.mock.log_generator import generate_dns_log_line

kafka_producer = SimpleKafkaProduceHandler()

if __name__ == "__main__":
    try:
        for _ in range(5000):
            kafka_producer.produce(
                "pipeline.logserver_in", f"{generate_dns_log_line('random-ip.de')}"
            )
            # time.sleep(0.1 * random.uniform(0.1, 1))
            # print(f"{generate_dns_log_line('random-ip.de')}")
    except KeyboardInterrupt:
        pass
