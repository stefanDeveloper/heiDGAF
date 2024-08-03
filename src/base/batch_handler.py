import json
import logging
import os
import sys
from asyncio import Lock
from threading import Timer

from src.base.kafka_handler import KafkaProduceHandler
from src.base.utils import setup_config, current_time

sys.path.append(os.getcwd())
from src.base.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

config = setup_config()
BATCH_SIZE = config["kafka"]["batch_sender"]["batch_size"]
BATCH_TIMEOUT = config["kafka"]["batch_sender"]["batch_timeout"]


# TODO: Test
class BufferedBatch:
    """
    Data structure for managing the batch, buffer, and timestamps. The batch contains the latest messages and has a
    timestamp marking the first time a message was added to it (or the previous batch was sent), and one
    for the time after the last message was added before sending. The buffer stores the previous batch messages
    including their timestamps.
    """

    def __init__(self):
        self.batch = {}  # Batch for the latest messages coming in
        self.buffer = {}  # Former batch with previous messages

        self.__begin_timestamps = {}
        self.__center_timestamps = {}
        self.__end_timestamps = {}

    def add_message(self, key: str, message: str) -> None:
        """
        Adds a given message to the messages list of the given key. If the key already exists, the message is simply
        added, otherwise, the key is created. In this case, a timestamp to mark the beginning of the batch is set.

        Args:
            key (str): Key to which the message is added
            message (str): Message to be added
        """
        if key in self.batch:  # key already has messages associated
            self.batch[key].append(message)
            logger.debug(f"Message '{message}' added to {key}'s batch.")
        else:  # key has no messages associated yet
            self.batch[key] = [message]
            logger.debug(f"Message '{message}' added to newly created {key}'s batch.")

            self.__center_timestamps[key] = current_time()
            logger.debug(f"center_timestamp of {key=} set to now: {self.__center_timestamps[key]}")

    def get_number_of_messages(self, key: str):
        """
        Returns the number of entries in the batch of the latest messages.

        Args:
            key (str): Key for which to return the number of messages

        Returns:
            Number of messages associated with the given key as integer
        """
        if key in self.batch:
            return len(self.batch[key])

        return 0

    def complete_batch(self, key: str) -> dict:
        """
        Completes the batch and returns a full data packet including timestamps and messages. Depending on the
        stored data, either both batches are added to the packet, or just the latest messages. Raises a ValueError
        if no data is available.

        Args:
            key (str): Key for which to complete the current batch and return data packet

        Returns:
            Dictionary of begin_timestamp, end_timestamp and messages (including buffered data) associated with a key

        Raises:
            ValueError: No data is available for sending.
        """
        if self.batch:
            if not self.buffer:  # Variant 1: Only batch has entries
                logger.debug("Variant 1: Only batch has entries. Sending...")
                begin_timestamp = self.__center_timestamps[key]
                buffer_data = []
            else:  # Variant 2: Buffer and batch have entries
                logger.debug("Variant 2: Buffer and batch have entries. Sending...")
                begin_timestamp = self.__begin_timestamps[key]
                buffer_data = self.buffer[key]

            self.__end_timestamps[key] = current_time()
            logger.debug(f"end_timestamp set to now: {self.__end_timestamps[key]}")

            data = {
                "begin_timestamp": begin_timestamp,
                "end_timestamp": self.__end_timestamps[key],
                "data": buffer_data + self.batch[key],
            }

            # Move data from batch to buffer
            self.buffer[key] = self.batch[key]
            del self.batch[key]
            # Move timestamps
            self.__begin_timestamps[key] = self.__center_timestamps[key]
            self.__center_timestamps[key] = self.__end_timestamps[key]
            return data

        if self.buffer:  # Variant 3: Only buffer has entries
            logger.debug("Variant 3: Only buffer has entries.")
            logger.debug("Deleting buffer data (has no influence on analysis since it was too long ago)...")
            self.buffer = {}
        else:  # Variant 4: No data exists
            logger.debug("Variant 4: No data exists. Nothing to send.")

        raise ValueError("No data available for sending.")

    def get_stored_keys(self) -> set:
        """
        Retrieve all keys stored in either the batch or the buffer.

        Returns:
            List of all keys stored in either dictionary
        """
        keys_set = set()

        for key in self.batch:
            keys_set.add(key)

        for key in self.buffer:
            keys_set.add(key)

        return keys_set


class CollectorKafkaBatchSender:
    """
    Adds messages to the :class:`BufferedBatch` and sends them after a timer ran out or the respective batch is full.
    """

    def __init__(self):
        self.topic = "Prefilter"
        self.batch = BufferedBatch()
        self.timer = None

        logger.debug(f"Calling KafkaProduceHandler(transactional_id='collector')...")
        self.kafka_produce_handler = KafkaProduceHandler(transactional_id="collector")
        logger.debug(f"Initialized KafkaBatchSender.")

    def __del__(self):
        logger.debug(f"Closing KafkaBatchSender ({self.topic=})...")
        if self.timer:
            logger.debug("Timer is active. Cancelling timer...")
            self.timer.cancel()
            logger.debug("Timer cancelled.")

        self._send_all_batches(reset_timer=False)

        logger.debug(f"Closed KafkaBatchSender ({self.topic=}).")

    def add_message(self, key: str, message: str) -> None:
        """
        Adds the given message to the key specific batch. Checks if the batch is full. If so, it is sent. In the first
        execution, the timer starts. The same timer is used for all keys.

        Args:
            message (str): Message to be added to the batch
            key (str): Key of the message (e.g. subnet of client IP address in a log message)
        """
        logger.debug(f"Adding message '{message}' to batch.")

        self.batch.add_message(key, message)

        logger.info(f"Batch: {self.batch.batch}")

        if self.batch.get_number_of_messages(key) >= BATCH_SIZE:
            logger.debug(f"Batch for {key=} is full. Calling _send_batch_for_key({key})...")
            self._send_batch_for_key(key)
        elif not self.timer:  # First time setting the timer
            logger.debug("Timer not set yet. Calling _reset_timer()...")
            self._reset_timer()

        logger.debug(f"Message '{message}' successfully added to batch for {key=}.")

    def _send_all_batches(self, reset_timer: bool = True) -> None:
        for key in self.batch.get_stored_keys():
            self._send_batch_for_key(key)

        if reset_timer:
            self._reset_timer()

    def _send_batch_for_key(self, key: str) -> None:
        logger.debug(f"Starting to send the batch for {key=}...")

        try:
            data_packet = self.batch.complete_batch(key)
        except ValueError as e:
            logger.debug(e)
            return

        self._send_data_packet(key, data_packet)

    def _send_data_packet(self, key: str, data: dict) -> None:
        logger.debug("Sending data to KafkaProduceHandler...")
        logger.debug(f"{data=}")
        self.kafka_produce_handler.send(
            topic=self.topic,
            data=json.dumps(data),
            key=key,
        )
        logger.debug(f"{data=}")

    def _reset_timer(self) -> None:
        """
        Resets an existing or starts a new timer with the globally set batch timeout. In the case of a timeout,
        all batches are sent. The timer serves as a backup so that all batches are cleared sometimes (prevents subnets
        with few messages from never being analyzed).
        """
        logger.debug("Resetting timer...")
        if self.timer:
            logger.debug("Cancelling active timer...")
            self.timer.cancel()
        else:
            logger.debug("No timer active.")

        logger.debug("Starting new timer...")
        self.timer = Timer(BATCH_TIMEOUT, self._send_all_batches)
        self.timer.start()
        logger.info("Successfully started new timer.")
