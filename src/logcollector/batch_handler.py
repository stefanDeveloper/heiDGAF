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
from src.base.utils import (
    setup_config,
    get_batch_configuration,
    generate_collisions_resistant_uuid,
)
from src.base.log_config import get_logger

module_name = "log_collection.batch_handler"
logger = get_logger(module_name)

config = setup_config()

KAFKA_BROKERS = ",".join(
    [
        f"{broker['hostname']}:{broker['port']}"
        for broker in config["environment"]["kafka_brokers"]
    ]
)


class BufferedBatch:
    """Data structure for managing the batch, buffer, and timestamps. The batch contains the latest messages and a
    buffer that stores the previous batch messages. Sorts the batches and can return timestamps.
    """

    def __init__(self, collector_name):
        self.name = f"buffered-batch-for-{collector_name}"
        self.batch = {}  # Batch for the latest messages coming in
        self.buffer = {}  # Former batch with previous messages
        self.batch_id = {}  # Batch ID per key

        # databases
        self.logline_to_batches = ClickHouseKafkaSender("logline_to_batches")
        self.batch_timestamps = ClickHouseKafkaSender("batch_timestamps")
        self.fill_levels = ClickHouseKafkaSender("fill_levels")
        self.batch_tree = ClickHouseKafkaSender("batch_tree")
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
        """Adds message to the key. If the key does not exist yet, it is created first.

        Args:
            key (str): Key to which the message is added
            logline_id (uuid.UUID): Logline ID of the message
            message (str): Message to be added
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
                    instance_name=self.name,
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
                    instance_name=self.name,
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
        """Returns the number of all batch entries as a sum over all key's batch entries."""
        return sum(len(key_entry) for key_entry in self.batch.values())

    def get_message_count_for_buffer(self) -> int:
        """Returns the number of all buffered entries as a sum over all key's buffer entries."""
        return sum(len(key_entry) for key_entry in self.buffer.values())

    def get_message_count_for_batch_key(self, key: str) -> int:
        """Returns the number of all batch messages for a given key.

        Args:
            key (str): Key for which message count is returned
        """
        if key in self.batch:
            return len(self.batch[key])

        return 0

    def get_message_count_for_buffer_key(self, key: str) -> int:
        """Returns the number of all buffered messages for a given key.

        Args:
            key (str): Key for which message count is returned
        """
        if key in self.buffer:
            return len(self.buffer[key])

        return 0

    def complete_batch(self, key: str) -> dict:
        """Completes the batch and returns a full data packet including timestamps and messages. Depending on the
        stored data, either both batches are added to the packet, or just the latest messages.

        Args:
            key (str): Key for which to complete the current batch and return data packet

        Returns:
            Set of new Logline IDs and dictionary of begin_timestamp, end_timestamp and messages (including buffered
            data) associated with a key

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
                        # TODO remove hardcoded ts value
                        json.loads(entries[0])["ts"]
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
                        json.loads(entries[0])["ts"] if entries and entries[0] else None
                    )

                begin_timestamp = _get_first_timestamp_of_buffer()

            batch_id = self.batch_id.get(key)

            def _get_last_timestamp_of_batch() -> str | None:
                entries = self.batch.get(key)

                return (
                    json.loads(entries[-1])["ts"] if entries and entries[-1] else None
                )

            row_id = generate_collisions_resistant_uuid()

            data = {
                "batch_tree_row_id": row_id,
                "batch_id": batch_id,
                "begin_timestamp": datetime.datetime.fromisoformat(begin_timestamp),
                "end_timestamp": datetime.datetime.fromisoformat(
                    _get_last_timestamp_of_batch()
                ),
                "data": buffer_data + self.batch[key],
            }

            timestamp = datetime.datetime.now()
            self.batch_timestamps.insert(
                dict(
                    batch_id=batch_id,
                    stage=module_name,
                    instance_name=self.name,
                    status="completed",
                    timestamp=timestamp,
                    is_active=True,
                    message_count=self.get_message_count_for_batch_key(key),
                )
            )
            self.batch_tree.insert(
                dict(
                    batch_row_id=row_id,
                    parent_batch_row_id=None,
                    stage=module_name,
                    instance_name=self.name,
                    timestamp=timestamp,
                    status="completed",
                    batch_id=batch_id,
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

        return keys_set.copy()

    @staticmethod
    def _extract_tuples_from_json_formatted_strings(
        data: list[str],
    ) -> list[tuple[str, str]]:
        tuples = []

        for item in data:
            record = json.loads(item)
            # TODO remove hardcoded ts value
            timestamp = record.get("ts", "")
            tuples.append((str(timestamp), item))

        return tuples

    @staticmethod
    def _sort_by_timestamp(
        data: list[tuple[str, str]],
    ) -> list[str]:
        sorted_data = sorted(data, key=lambda x: x[0])
        loglines = [message for _, message in sorted_data]

        return loglines

    def _sort_batch(self, key: str):
        if key in self.batch:
            self.batch[key] = self._sort_by_timestamp(
                self._extract_tuples_from_json_formatted_strings(self.batch[key])
            )

    def _sort_buffer(self, key: str):
        if key in self.buffer:
            self.buffer[key] = self._sort_by_timestamp(
                self._extract_tuples_from_json_formatted_strings(self.buffer[key])
            )


class BufferedBatchSender:
    """Adds messages to the :class:`BufferedBatch` and sends them after a timer ran out or a key's batch is full."""

    def __init__(self, produce_topics, collector_name):
        self.topics = produce_topics
        self.batch_configuration = get_batch_configuration(collector_name)
        self.batch = BufferedBatch(collector_name)
        self.timer = None

        self.kafka_produce_handler = ExactlyOnceKafkaProduceHandler()

        # databases
        self.logline_timestamps = ClickHouseKafkaSender("logline_timestamps")

    def __del__(self):
        if self.timer:
            self.timer.cancel()

        self._send_all_batches(reset_timer=False)

    def add_message(self, key: str, message: str) -> None:
        """Adds the message to the key's batch and sends it if it is full. In the first execution, a timer starts
        whose timeout triggers sending for all batches.

        Args:
            message (str): Message to be added to the batch
            key (str): Key of the message (e.g. subnet of client IP address in a log message)
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

        if number_of_messages_for_key >= self.batch_configuration["batch_size"]:
            self._send_batch_for_key(key)
            logger.info(
                f"Full batch: Successfully sent batch messages for subnet_id {key}.\n"
                f"    ⤷  {number_of_messages_for_key} messages sent."
            )
        elif not self.timer:  # First time setting the timer
            self._reset_timer()

    def _send_all_batches(self, reset_timer: bool = True) -> None:
        """
        Dispatch all batches for the Kafka queue

        Args:
            reset_timer (bool): whether the timer should be reset
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
        """
        Send one batch based on the key

        Args:
            key (str): Key to identify the batch
        """
        try:
            data = self.batch.complete_batch(key)
        except ValueError as e:
            logger.debug(e)
            return

        self._send_data_packet(key, data)

    def _send_data_packet(self, key: str, data: dict) -> None:
        """
        Sends a packet of a Batch to the defined Kafka topics

        Args:
            key (str): key to identify the batch
            data (dict): the batch data to send
        """
        batch_schema = marshmallow_dataclass.class_schema(Batch)()

        for topic in self.topics:
            self.kafka_produce_handler.produce(
                topic=topic,
                data=batch_schema.dumps(data),
                key=key,
            )
            logger.info(f"send data to {topic}")

    def _reset_timer(self) -> None:
        """Restarts the internal timer of the object"""
        if self.timer:
            self.timer.cancel()

        self.timer = Timer(
            self.batch_configuration["batch_timeout"], self._send_all_batches
        )
        self.timer.start()
