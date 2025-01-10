import os
import sys

sys.path.append(os.getcwd())
from src.base.kafka_handler import SimpleKafkaProduceHandler
from src.mock.log_generator import generate_dns_log_line

kafka_producer = SimpleKafkaProduceHandler()


def main():
    try:
        for i in range(20):
            kafka_producer.produce(
                "pipeline-logserver_in", f"{generate_dns_log_line('random-ip.de')}"
            )
            print("Sent logline", i)
            # time.sleep(0.1 * random.uniform(0.1, 1))
            # print(f"{generate_dns_log_line('random-ip.de')}")
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
