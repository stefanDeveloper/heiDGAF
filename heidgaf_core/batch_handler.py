import json
import logging
import os  # needed for Terminal execution
import sys  # needed for Terminal execution
from threading import Timer, Lock

from heidgaf_core.kafka_handler import KafkaProducerWrapper

sys.path.append(os.getcwd())  # needed for Terminal execution
from heidgaf_core.log_config import setup_logging
from heidgaf_core.config import *

setup_logging()
logger = logging.getLogger(__name__)


class KafkaBatchSender:
    def __init__(self, topic: str, transactional_id: str):
        self.topic = topic
        self.messages = []
        self.lock = Lock()
        self.timer = None
        self.kafka_produce_handler = KafkaProducerWrapper(transactional_id=transactional_id)

    def _send_batch(self):
        with self.lock:
            if self.messages:
                self.kafka_produce_handler.send(
                    topic=self.topic,
                    data=json.dumps(self.messages),
                )
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
