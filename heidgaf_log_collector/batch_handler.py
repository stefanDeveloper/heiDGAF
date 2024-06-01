import json
import logging
import os  # needed for Terminal execution
import sys  # needed for Terminal execution
from threading import Timer, Lock

from confluent_kafka import Producer

sys.path.append(os.getcwd())  # needed for Terminal execution
from heidgaf_log_collector.utils import kafka_delivery_report
from pipeline_prototype.logging_config import setup_logging
from heidgaf_log_collector.config import *

setup_logging()
logger = logging.getLogger(__name__)


class KafkaBatchSender:
    def __init__(self, topic: str):
        self.topic = topic
        self.messages = []
        self.lock = Lock()
        self.timer = None
        self.kafka_producer = None

    def start_kafka_producer(self):
        if self.kafka_producer:
            logger.warning(f"Kafka Producer already running on {KAFKA_BROKER_HOST}:{KAFKA_BROKER_PORT}.")
            return

        conf = {'bootstrap.servers': f"{KAFKA_BROKER_HOST}:{KAFKA_BROKER_PORT}"}
        self.kafka_producer = Producer(conf)

    def _send_batch(self):
        if not self.kafka_producer:
            logger.error(f"Kafka Producer not running!")
            return

        with self.lock:
            if self.messages:
                self.kafka_producer.produce(
                    topic=self.topic,
                    key=None,  # could maybe add a key here
                    value=json.dumps(self.messages).encode('utf-8'),
                    callback=kafka_delivery_report,
                )
                self.kafka_producer.flush()
                self.messages = []
            self._reset_timer()

    def _reset_timer(self):
        if self.timer:
            self.timer.cancel()
        self.timer = Timer(BATCH_TIMEOUT, self._send_batch)
        self.timer.start()

    def add_message(self, message: str):
        with self.lock:
            self.messages.append(message)
            if len(self.messages) >= BATCH_SIZE:
                self._send_batch()
            elif not self.timer:
                self._reset_timer()

    def close(self):
        if self.timer:
            self.timer.cancel()
        self._send_batch()
