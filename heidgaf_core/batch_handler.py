import json
import logging
import os  # needed for Terminal execution
import sys  # needed for Terminal execution
from threading import Timer, Lock

from heidgaf_core.kafka_handler import KafkaProduceHandler

sys.path.append(os.getcwd())  # needed for Terminal execution
from heidgaf_core.log_config import setup_logging
from heidgaf_core.config import *

setup_logging()
logger = logging.getLogger(__name__)


class KafkaBatchSender:
    def __init__(self, topic: str, transactional_id: str, buffer: bool = False):
        self.topic = topic
        self.latest_messages = []
        self.earlier_messages = []
        self.buffer = buffer
        self.lock = Lock()
        self.timer = None
        self.kafka_produce_handler = KafkaProduceHandler(transactional_id=transactional_id)

    def add_message(self, message: str):
        with self.lock:
            self.latest_messages.append(message)

            if len(self.latest_messages) >= BATCH_SIZE:
                self._send_batch()
            elif not self.timer:
                self._reset_timer()

    def close(self):
        if self.timer:
            self.timer.cancel()

        self._send_batch()

    def _send_batch(self):
        with self.lock:
            if self.earlier_messages or self.latest_messages:
                self.kafka_produce_handler.send(
                    topic=self.topic,
                    data=json.dumps(self.earlier_messages + self.latest_messages),
                )

                if self.buffer:
                    self.earlier_messages = self.latest_messages

                self.latest_messages = []

            self._reset_timer()

    def _reset_timer(self):
        if self.timer:
            self.timer.cancel()

        self.timer = Timer(BATCH_TIMEOUT, self._send_batch)
        self.timer.start()
