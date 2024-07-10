import json
import logging
import os  # needed for Terminal execution
import sys  # needed for Terminal execution
import time
from threading import Lock, Timer

from src.base.kafka_handler import KafkaProduceHandler
from src.base.utils import current_time, setup_config

sys.path.append(os.getcwd())  # needed for Terminal execution
from src.base.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

config = setup_config()
BATCH_SIZE = config["kafka"]["batch_sender"]["batch_size"]
BATCH_TIMEOUT = config["kafka"]["batch_sender"]["batch_timeout"]


class KafkaBatchSender:
    def __init__(self, topic: str, transactional_id: str, buffer: bool = False):
        logger.debug(
            f"Initializing KafkaBatchSender ({topic=}, {transactional_id=} and {buffer=})..."
        )
        self.topic = topic
        self.latest_messages = []
        self.earlier_messages = []
        self.buffer = buffer
        self.lock = Lock()
        self.timer = None
        self.begin_timestamp = None
        self.center_timestamp = None
        self.end_timestamp = None
        logger.debug(f"Calling KafkaProduceHandler({transactional_id=})...")
        self.kafka_produce_handler = KafkaProduceHandler(
            transactional_id=transactional_id
        )
        logger.debug(
            f"Initialized KafkaBatchSender ({topic=}, {transactional_id=} and {buffer=})."
        )

    def add_message(self, message: str):
        logger.debug(f"Adding message '{message}' to batch.")
        with self.lock:
            self.latest_messages.append(message)

            if len(self.latest_messages) >= BATCH_SIZE:
                logger.debug("Batch is full. Calling _send_batch()...")
                self._send_batch()
            elif not self.timer:  # First time setting the timer
                logger.debug("Timer not set yet. Calling _reset_timer()...")
                self.begin_timestamp = current_time()
                logger.debug(f"begin_timestamp set to '{self.begin_timestamp}'")
                self._reset_timer()
        logger.debug(f"Message '{message}' successfully added to batch.")

    def close(self):  # TODO: Change to __del__
        logger.debug(f"Closing KafkaBatchSender ({self.topic=} and {self.buffer=})...")
        if self.timer:
            logger.debug("Timer is active. Cancelling timer...")
            self.timer.cancel()
            logger.debug("Timer cancelled.")

        logger.debug("Calling _send_batch()...")
        self._send_batch()
        logger.debug(f"Closed KafkaBatchSender ({self.topic=} and {self.buffer=}).")

    def _send_batch(self):
        logger.debug("Starting to send the batch...")

        if self.earlier_messages or self.latest_messages:
            logger.debug(
                "Messages not empty. Trying to send batch to KafkaProduceHandler..."
            )

            if not self.buffer:
                self.begin_timestamp = self.end_timestamp
                logger.debug(f"begin_timestamp set to former end_timestamp: {self.begin_timestamp}")

            self.end_timestamp = current_time()
            logger.debug(f"end_timestamp set to now: {self.end_timestamp}")

            data_to_send = {
                "begin_timestamp": self.begin_timestamp,
                "end_timestamp": self.end_timestamp,
                "data": self.earlier_messages + self.latest_messages,
            }
            logger.debug(f"{data_to_send=}")
            logger.debug(f"{json.dumps(data_to_send)=}")
            self.kafka_produce_handler.send(
                topic=self.topic,
                data=json.dumps(data_to_send),
            )

            if self.buffer:
                logger.debug("Storing earlier messages in buffer...")
                if self.center_timestamp:
                    self.begin_timestamp = self.center_timestamp
                    logger.debug(f"begin_timestamp set to former center_timestamp: {self.begin_timestamp}")
                self.earlier_messages = self.latest_messages
                self.center_timestamp = self.end_timestamp
                logger.debug(f"center_timestamp set to former end_timestamp: {self.center_timestamp}")
                self.end_timestamp = None
                logger.debug(f"end_timestamp set to {self.end_timestamp}")
                logger.debug("Earlier messages stored in buffer.")

            self.latest_messages = []
        else:
            logger.debug("Messages are empty. Nothing to send.")
            return

        logger.debug("Calling _reset_timer()...")
        self._reset_timer()
        logger.info("Batch successfully sent.")

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


# TODO: Test
class CollectorKafkaBatchSender(KafkaBatchSender):
    def __init__(self, transactional_id: str):
        logger.debug("Calling KafkaBatchSender(topic='Prefilter', transactional_id=transactional_id, buffer=True)...")
        super().__init__(topic="Prefilter", transactional_id=transactional_id, buffer=True)
        logger.debug(f"Initialized CollectorKafkaBatchSender ({transactional_id=}).")


if __name__ == "__main__":
    instance = KafkaBatchSender("test_topic", "test_id", True)
    instance.add_message("message_1")
    time.sleep(2)
    instance.add_message("message_2")
    time.sleep(2)
    instance.add_message("message_3")
    time.sleep(2)
    instance.add_message("message_4")
    time.sleep(2)
    instance.add_message("message_5")
    time.sleep(2)
    instance.add_message("message_6")
    instance.close()
