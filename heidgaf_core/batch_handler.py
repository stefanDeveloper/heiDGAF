import json
import logging
import os  # needed for Terminal execution
import sys  # needed for Terminal execution
from threading import Timer, Lock

sys.path.append(os.getcwd())  # needed for Terminal execution
from heidgaf_core.kafka_handler import KafkaProduceHandler
from heidgaf_core.log_config import setup_logging
from heidgaf_core.config import *

setup_logging()
logger = logging.getLogger(__name__)


class KafkaBatchSender:
    def __init__(self, topic: str, transactional_id: str, buffer: bool = False):
        logger.debug(f"Initializing {self.__class__.__name__} ({topic=}, {transactional_id=} and {buffer=})...")
        self.topic = topic
        self.latest_messages = []
        self.earlier_messages = []
        self.buffer = buffer
        self.lock = Lock()
        self.timer = None
        self.kafka_produce_handler = KafkaProduceHandler(transactional_id=transactional_id)
        logger.debug(f"Initialized {self.__class__.__name__} ({topic=}, {transactional_id=} and {buffer=}).")

    def add_message(self, message: str):
        logger.debug(f"Adding message '{message}' to batch.")
        with self.lock:
            self.latest_messages.append(message)

            if len(self.latest_messages) >= BATCH_SIZE:
                logger.debug("Batch is full. Calling _send_batch()...")
                self._send_batch()
            elif not self.timer:
                logger.debug("Timer not set yet. Calling _reset_timer()...")
                self._reset_timer()
        logger.debug(f"Message '{message}' successfully added to batch.")

    def close(self):  # TODO: Change to __del__
        logger.debug(f"Closing {self.__class__.__name__} ({self.topic=} and {self.buffer=})...")
        if self.timer:
            logger.debug("Timer is active. Cancelling timer...")
            self.timer.cancel()
            logger.debug("Timer cancelled.")

        logger.debug("Calling _send_batch()...")
        self._send_batch()
        logger.debug(f"Closed {self.__class__.__name__} ({self.topic=} and {self.buffer=}).")

    def _send_batch(self):
        logger.debug("Starting to send the batch...")
        with self.lock:
            if self.earlier_messages or self.latest_messages:
                logger.debug("Messages not empty. Trying to send batch to KafkaProduceHandler...")
                self.kafka_produce_handler.send(
                    topic=self.topic,
                    data=json.dumps(self.earlier_messages + self.latest_messages),
                )

                if self.buffer:
                    logger.debug("Storing earlier messages in buffer...")
                    self.earlier_messages = self.latest_messages
                    logger.debug("Earlier messages stored in buffer.")

                self.latest_messages = []
            else:
                logger.debug("Messages are empty. Nothing to send.")

            logger.debug("Resetting timer...")
            self._reset_timer()
        logger.debug("Batch successfully sent or nothing to send.")

    def _reset_timer(self):
        logger.debug("Resetting timer...")
        if self.timer:
            logger.debug("Cancelling active timer...")
            self.timer.cancel()
        else:
            logger.debug("No timer active.")

        logger.debug("Starting new timer...")
        self.timer = Timer(BATCH_TIMEOUT, self._send_batch)
        self.timer.start()
        logger.debug("Successfully started new timer.")


if __name__ == '__main__':
    instance = KafkaBatchSender("topic", "tr_id")
    instance._send_batch()
