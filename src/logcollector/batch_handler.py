import datetime
import json
import os
import sys
import uuid
from threading import Timer

import marshmallow_dataclass

sys.path.append(os.getcwd())
from src.base.data_classes.batch import Batch
from src.base.clickhouse_kafka_sender import ClickHouseKafkaSender
from src.base.kafka_handler import ExactlyOnceKafkaProduceHandler
from src.base.utils import setup_config
from src.base.log_config import get_logger

module_name = "log_collection.batch_handler"
logger = get_logger(module_name)

config = setup_config()
BATCH_SIZE = config["pipeline"]["log_collection"]["batch_handler"]["batch_size"]
BATCH_TIMEOUT = config["pipeline"]["log_collection"]["batch_handler"]["batch_timeout"]
PRODUCE_TOPIC = config["environment"]["kafka_topics"]["pipeline"][
    "batch_sender_to_prefilter"
]
KAFKA_BROKERS = ",".join(
    [
        f"{broker['hostname']}:{broker['port']}"
        for broker in config["environment"]["kafka_brokers"]
    ]
)


class BufferedBatch:
    """Data structure for managing batches, buffers, and timestamps in the log collection pipeline

    Manages two data structures: a current batch that collects incoming messages and a buffer that stores
    previously processed batch messages. The batch groups messages by key (typically subnet ID) and handles
    automatic sending when size or timeout limits are reached. All batches are sorted by timestamp to ensure
    chronological processing. Tracks batch metadata including IDs, timestamps, and fill levels for monitoring.
    """

    def __init__(self):
        self.batch = {}  # Batch for the latest messages coming in
        self.buffer = {}  # Former batch with previous messages
        self.batch_id = {}  # Batch ID per key

        # databases
        self.logline_to_batches = ClickHouseKafkaSender("logline_to_batches")
        self.batch_timestamps = ClickHouseKafkaSender("batch_timestamps")
        self.fill_levels = ClickHouseKafkaSender("fill_levels")

        self.fill_levels.insert(
            dict(
                timestamp=datetime.datetime.now(),
                stage=module_name,
                entry_type="total_loglines_in_batches",
                entry_count=0,
            )
        )

        self.fill_levels.insert(
            dict(
                timestamp=datetime.datetime.now(),
                stage=module_name,
                entry_type="total_loglines_in_buffer",
                entry_count=0,
            )
        )

    def add_message(self, key: str, logline_id: uuid.UUID, message: str) -> None:
        """Adds a message to the batch associated with the given key.

        If the key does not exist in the current batch, a new batch entry is created with a unique batch ID.
        For existing keys, the message is appended to the existing batch. Logs the association between the
        logline and batch ID, updates batch timestamps, and tracks fill levels for monitoring purposes.

        Args:
            key (str): Key to which the message is added (typically subnet ID).
            logline_id (uuid.UUID): Unique identifier of the logline message.
            message (str): JSON-formatted message to be added to the batch.
        """
        if key in self.batch:  # key already has messages associated
            self.batch[key].append(message)

            batch_id = self.batch_id.get(key)
            self.logline_to_batches.insert(
                dict(
                    logline_id=logline_id,
                    batch_id=batch_id,
                )
            )

            self.batch_timestamps.insert(
                dict(
                    batch_id=batch_id,
                    stage=module_name,
                    status="waiting",
                    timestamp=datetime.datetime.now(),
                    is_active=True,
                    message_count=self.get_message_count_for_batch_key(key),
                )
            )

        else:  # key has no messages associated yet
            # create new batch
            self.batch[key] = [message]
            new_batch_id = uuid.uuid4()
            self.batch_id[key] = new_batch_id

            self.logline_to_batches.insert(
                dict(
                    logline_id=logline_id,
                    batch_id=new_batch_id,
                )
            )

            self.batch_timestamps.insert(
                dict(
                    batch_id=new_batch_id,
                    stage=module_name,
                    status="waiting",
                    timestamp=datetime.datetime.now(),
                    is_active=True,
                    message_count=1,
                )
            )

        self.fill_levels.insert(
            dict(
                timestamp=datetime.datetime.now(),
                stage=module_name,
                entry_type="total_loglines_in_batches",
                entry_count=self.get_message_count_for_batch(),
            )
        )

    def get_message_count_for_batch(self) -> int:
        """Returns the total number of messages across all batches.

        Calculates the sum of message counts from all key-specific batches currently stored.

        Returns:
            Total number of messages in all batches.
        """
        return sum(len(key_entry) for key_entry in self.batch.values())

    def get_message_count_for_buffer(self) -> int:
        """Returns the total number of messages across all buffers.

        Calculates the sum of message counts from all key-specific buffers currently stored.

        Returns:
            Total number of messages in all buffers.
        """
        return sum(len(key_entry) for key_entry in self.buffer.values())

    def get_message_count_for_batch_key(self, key: str) -> int:
        """Returns the number of messages in the batch for a specific key.

        Args:
            key (str): Key for which message count is returned.

        Returns:
            Number of messages in the batch for the given key, or 0 if key doesn't exist.
        """
        if key in self.batch:
            return len(self.batch[key])

        return 0

    def get_message_count_for_buffer_key(self, key: str) -> int:
        """Returns the number of messages in the buffer for a specific key.

        Args:
            key (str): Key for which message count is returned.

        Returns:
            Number of messages in the buffer for the given key, or 0 if key doesn't exist.
        """
        if key in self.buffer:
            return len(self.buffer[key])

        return 0

    def complete_batch(self, key: str) -> dict:
        """Completes the batch for a specific key and returns a formatted data packet.

        Handles multiple scenarios based on available data:
        - Variant 1: Only batch has entries - sends current batch data
        - Variant 2: Both buffer and batch have entries - combines both in chronological order
        - Variant 3: Only buffer has entries - cleans up old buffer data
        - Variant 4: No data exists - raises ValueError

        The method sorts all messages by timestamp, creates a data packet with batch metadata,
        logs completion timestamps, and moves current batch data to buffer for next iteration.

        Args:
            key (str): Key for which to complete the current batch and return data packet.

        Returns:
            Dictionary containing batch_id, begin_timestamp, end_timestamp, and chronologically
            sorted message data combining buffer and batch messages.

        Raises:
            ValueError: No data is available for sending.
        """
        if self.batch.get(key):
            if not self.buffer.get(key):  # Variant 1: Only batch has entries
                logger.debug("Variant 1: Only batch has entries. Sending...")
                self._sort_batch(key)
                buffer_data = []

                def _get_first_timestamp_of_batch() -> str | None:
                    entries = self.batch.get(key)
                    return (
                        json.loads(entries[0])["timestamp"]
                        if entries and entries[0]
                        else None
                    )

                begin_timestamp = _get_first_timestamp_of_batch()

            else:  # Variant 2: Buffer and batch have entries
                logger.debug("Variant 2: Buffer and batch have entries. Sending...")
                self._sort_batch(key)
                self._sort_buffer(key)
                buffer_data = self.buffer[key]

                def _get_first_timestamp_of_buffer() -> str | None:
                    entries = self.buffer.get(key)
                    return (
                        json.loads(entries[0])["timestamp"]
                        if entries and entries[0]
                        else None
                    )

                begin_timestamp = _get_first_timestamp_of_buffer()

            batch_id = self.batch_id.get(key)

            def _get_last_timestamp_of_batch() -> str | None:
                entries = self.batch.get(key)

                return (
                    json.loads(entries[-1])["timestamp"]
                    if entries and entries[-1]
                    else None
                )

            data = {
                "batch_id": batch_id,
                "begin_timestamp": datetime.datetime.fromisoformat(begin_timestamp),
                "end_timestamp": datetime.datetime.fromisoformat(
                    _get_last_timestamp_of_batch()
                ),
                "data": buffer_data + self.batch[key],
            }

            self.batch_timestamps.insert(
                dict(
                    batch_id=batch_id,
                    stage=module_name,
                    status="completed",
                    timestamp=datetime.datetime.now(),
                    is_active=True,
                    message_count=self.get_message_count_for_batch_key(key),
                )
            )

            # Move data from batch to buffer
            self.buffer[key] = self.batch[key]
            del self.batch[key]

            # Batch ID is not needed anymore
            del self.batch_id[key]

            self.fill_levels.insert(
                dict(
                    timestamp=datetime.datetime.now(),
                    stage=module_name,
                    entry_type="total_loglines_in_batches",
                    entry_count=self.get_message_count_for_batch(),
                )
            )

            self.fill_levels.insert(
                dict(
                    timestamp=datetime.datetime.now(),
                    stage=module_name,
                    entry_type="total_loglines_in_buffer",
                    entry_count=self.get_message_count_for_buffer(),
                )
            )

            return data

        if self.buffer.get(key):  # Variant 3: Only buffer has entries
            logger.debug("Variant 3: Only buffer has entries.")
            logger.debug(
                "Deleting buffer data (has no influence on analysis since it was too long ago)..."
            )

            del self.buffer[key]

            self.fill_levels.insert(
                dict(
                    timestamp=datetime.datetime.now(),
                    stage=module_name,
                    entry_type="total_loglines_in_buffer",
                    entry_count=self.get_message_count_for_buffer(),
                )
            )

        else:  # Variant 4: No data exists
            logger.debug("Variant 4: No data exists. Nothing to send.")

        raise ValueError("No data available for sending.")

    def get_stored_keys(self) -> set:
        """Retrieves all keys stored in either the batch or the buffer.

        Combines keys from both the current batch dictionary and the buffer dictionary
        to provide a complete set of all keys that have associated data.

        Returns:
            Set of all unique keys stored in either batch or buffer dictionaries.
        """
        keys_set = set()

        for key in self.batch:
            keys_set.add(key)

        for key in self.buffer:
            keys_set.add(key)

        return keys_set.copy()

    @staticmethod
    def _extract_tuples_from_json_formatted_strings(
        data: list[str],
    ) -> list[tuple[str, str]]:
        """Extracts timestamp-message tuples from JSON-formatted message strings.

        Parses each JSON string to extract the timestamp field and creates tuples
        containing the timestamp and the original message for sorting purposes.

        Args:
            data (list[str]): List of JSON-formatted message strings.

        Returns:
            List of tuples containing (timestamp, message) pairs.
        """
        tuples = []

        for item in data:
            record = json.loads(item)

            timestamp = record.get("timestamp", "")
            tuples.append((str(timestamp), item))

        return tuples

    @staticmethod
    def _sort_by_timestamp(
        data: list[tuple[str, str]],
    ) -> list[str]:
        """Sorts message tuples by timestamp and returns the sorted messages.

        Takes a list of (timestamp, message) tuples, sorts them chronologically
        by timestamp, and extracts the sorted messages.

        Args:
            data (list[tuple[str, str]]): List of (timestamp, message) tuples.

        Returns:
            List of messages sorted chronologically by timestamp.
        """
        sorted_data = sorted(data, key=lambda x: x[0])
        loglines = [message for _, message in sorted_data]

        return loglines

    def _sort_batch(self, key: str):
        """Sorts the batch messages for a specific key by timestamp.

        Extracts timestamps from JSON-formatted messages in the batch and sorts them
        chronologically. Updates the batch in-place with the sorted messages.

        Args:
            key (str): Key identifying the batch to be sorted.
        """
        if key in self.batch:
            self.batch[key] = self._sort_by_timestamp(
                self._extract_tuples_from_json_formatted_strings(self.batch[key])
            )

    def _sort_buffer(self, key: str):
        """Sorts the buffer messages for a specific key by timestamp.

        Extracts timestamps from JSON-formatted messages in the buffer and sorts them
        chronologically. Updates the buffer in-place with the sorted messages.

        Args:
            key (str): Key identifying the buffer to be sorted.
        """
        if key in self.buffer:
            self.buffer[key] = self._sort_by_timestamp(
                self._extract_tuples_from_json_formatted_strings(self.buffer[key])
            )


class BufferedBatchSender:
    """Main component for managing batch collection and dispatch in the log collection stage

    Coordinates the addition of messages to batches and handles automatic sending based on two triggers:
    size-based (when a batch reaches the configured size limit) or time-based (when a timeout expires).
    Manages a timer that ensures batches are sent even if they don't reach the size threshold,
    preventing data from being held indefinitely. Sends completed batches to the next pipeline stage
    via Kafka and tracks message timestamps for monitoring and debugging purposes.
    """

    def __init__(self):
        self.topic = PRODUCE_TOPIC
        self.batch = BufferedBatch()
        self.timer = None

        self.kafka_produce_handler = ExactlyOnceKafkaProduceHandler()

        # databases
        self.logline_timestamps = ClickHouseKafkaSender("logline_timestamps")

    def __del__(self):
        if self.timer:
            self.timer.cancel()

        self._send_all_batches(reset_timer=False)

    def add_message(self, key: str, message: str) -> None:
        """Adds a message to the batch and triggers sending if batch size limit is reached.

        Extracts the logline ID from the JSON message, logs the processing timestamps,
        and adds the message to the appropriate batch. If the batch reaches the configured
        size limit, it is immediately sent. On the first message, starts a timer that will
        trigger sending of all batches when the timeout expires.

        Args:
            key (str): Key of the message (typically subnet ID derived from client IP address).
            message (str): JSON-formatted message to be added to the batch.
        """
        logline_id = json.loads(message).get("logline_id")
        self.logline_timestamps.insert(
            dict(
                logline_id=logline_id,
                stage=module_name,
                status="in_process",
                timestamp=datetime.datetime.now(),
                is_active=True,
            )
        )

        self.batch.add_message(key, logline_id, message)
        self.logline_timestamps.insert(
            dict(
                logline_id=logline_id,
                stage=module_name,
                status="batched",
                timestamp=datetime.datetime.now(),
                is_active=True,
            )
        )

        logger.debug(f"Batch: {self.batch.batch}")
        number_of_messages_for_key = self.batch.get_message_count_for_batch_key(key)

        if number_of_messages_for_key >= BATCH_SIZE:
            self._send_batch_for_key(key)
            logger.info(
                f"Full batch: Successfully sent batch messages for subnet_id {key}.\n"
                f"    ⤷  {number_of_messages_for_key} messages sent."
            )
        elif not self.timer:  # First time setting the timer
            self._reset_timer()

    def _send_all_batches(self, reset_timer: bool = True) -> None:
        """Dispatches all available batches to the Kafka queue.

        Iterates through all stored keys and sends their associated batches. Provides
        detailed logging about the number of messages and batches sent. Optionally
        resets the internal timer after completion.

        Args:
            reset_timer (bool): Whether the timer should be reset after sending.
                                Default: True
        """
        number_of_keys = 0
        total_number_of_batch_messages = self.batch.get_message_count_for_batch()
        total_number_of_buffer_messages = self.batch.get_message_count_for_buffer()

        for key in self.batch.get_stored_keys():
            number_of_keys += 1
            self._send_batch_for_key(key)

        if reset_timer:
            self._reset_timer()

        if total_number_of_batch_messages == 0:
            return

        if number_of_keys == 1:
            logger.info(
                "Successfully sent all batches.\n"
                f"    ⤷  Batch for one subnet_id with "
                f"{total_number_of_batch_messages + total_number_of_buffer_messages} message(s) sent ("
                f"{total_number_of_batch_messages} batch message(s), {total_number_of_buffer_messages} "
                f"buffer message(s))."
            )
        else:  # if number_of_keys > 1
            logger.info(
                "Successfully sent all batches.\n"
                f"    ⤷  Batches for {number_of_keys} subnet_ids sent. "
                f"In total: {total_number_of_batch_messages + total_number_of_buffer_messages} messages ("
                f"{total_number_of_batch_messages} batch message(s), {total_number_of_buffer_messages} "
                f"buffer message(s))"
            )

    def _send_batch_for_key(self, key: str) -> None:
        """Sends a single batch for the specified key.

        Attempts to complete the batch for the given key and sends the resulting
        data packet. If no data is available for the key, the operation is skipped.

        Args:
            key (str): Key to identify the batch to be sent.
        """
        try:
            data = self.batch.complete_batch(key)
        except ValueError as e:
            logger.debug(e)
            return

        self._send_data_packet(key, data)

    def _send_data_packet(self, key: str, data: dict) -> None:
        """Sends a batch data packet to the configured Kafka topic.

        Serializes the batch data using the Batch schema and produces it to the
        configured Kafka topic with the associated key for proper partitioning.

        Args:
            key (str): Key to identify the batch for Kafka partitioning.
            data (dict): The batch data to be serialized and sent.
        """
        batch_schema = marshmallow_dataclass.class_schema(Batch)()

        self.kafka_produce_handler.produce(
            topic=self.topic,
            data=batch_schema.dumps(data),
            key=key,
        )

    def _reset_timer(self) -> None:
        """Restarts the internal timer for batch timeout handling.

        Cancels any existing timer and creates a new one with the configured timeout.
        When the timer expires, all available batches will be automatically sent.
        """
        if self.timer:
            self.timer.cancel()

        self.timer = Timer(BATCH_TIMEOUT, self._send_all_batches)
        self.timer.start()
