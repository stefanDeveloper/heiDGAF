import time
import unittest
from datetime import datetime, timedelta

from src.logcollector.batch_handler import BufferedBatch


class TestInit(unittest.TestCase):
    def test_init(self):
        # Act
        sut = BufferedBatch()

        # Assert
        self.assertEqual({}, sut.batch)
        self.assertEqual({}, sut.buffer)


class TestAddMessage(unittest.TestCase):
    def test_add_message_empty_batch_and_empty_buffer(self):
        # Arrange
        key = "test_key"
        message = "test_message"

        sut = BufferedBatch()

        # Act
        sut.add_message(key, message)

        # Assert
        self.assertEqual({key: [message]}, sut.batch, "Batch should contain the key with the message")
        self.assertEqual({}, sut.buffer, "Buffer should remain empty")

    def test_add_message_empty_batch_and_used_buffer(self):
        # Arrange
        key = "test_key"
        message = "test_message"
        old_message = "old_message"

        sut = BufferedBatch()
        sut.buffer = {key: [old_message]}

        # Act
        sut.add_message(key, message)

        # Assert
        self.assertEqual({key: [message]}, sut.batch, "Batch should contain the key with the message")
        self.assertEqual({key: [old_message]}, sut.buffer, "Buffer should still contain key with old message")

    def test_add_message_used_batch_and_empty_buffer(self):
        # Arrange
        key = "test_key"
        message = "test_message"
        old_message = "old_message"

        sut = BufferedBatch()
        sut.batch = {key: [old_message]}

        # Act
        sut.add_message(key, message)

        # Assert
        self.assertEqual({key: [old_message, message]}, sut.batch,
                         "Batch should contain the key with the old and new message")
        self.assertEqual({}, sut.buffer, "Buffer should remain empty")

    def test_add_message_used_batch_and_used_buffer(self):
        # Arrange
        key = "test_key"
        message = "test_message"
        old_message_1 = "old_message_1"
        old_message_2 = "old_message_2"

        sut = BufferedBatch()
        sut.batch = {key: [old_message_2]}
        sut.buffer = {key: [old_message_1]}

        # Act
        sut.add_message(key, message)

        # Assert
        self.assertEqual({key: [old_message_2, message]}, sut.batch,
                         "Batch should contain the key with the old and new message")
        self.assertEqual({key: [old_message_1]}, sut.buffer, "Buffer should still contain key with old message")

    def test_add_message_with_existing_other_key(self):
        # Arrange
        key = "test_key"
        message = "test_message"
        old_key = "old_key"
        old_message_1 = "old_message_1"
        old_message_2 = "old_message_2"

        sut = BufferedBatch()
        sut.batch = {old_key: [old_message_2]}
        sut.buffer = {old_key: [old_message_1]}

        # Act
        sut.add_message(key, message)

        # Assert
        self.assertEqual({old_key: [old_message_2], key: [message]}, sut.batch,
                         "Batch should contain the old key with the old message and the new key with the new message")
        self.assertEqual({old_key: [old_message_1]}, sut.buffer, "Buffer should still contain old key with old message")


class TestGetNumberOfMessages(unittest.TestCase):
    def test_get_number_of_messages_with_empty_batch(self):
        # Arrange
        key = "test_key"

        sut = BufferedBatch()

        # Act and Assert
        self.assertEqual(0, sut.get_number_of_messages(key))

    def test_get_number_of_messages_with_used_batch_for_key(self):
        # Arrange
        key = "test_key"
        message = "test_message"

        sut = BufferedBatch()
        sut.batch = {key: [message]}

        # Act and Assert
        self.assertEqual(1, sut.get_number_of_messages(key))

    def test_get_number_of_messages_with_used_batch_for_other_key(self):
        # Arrange
        key = "test_key"
        other_key = "other_key"
        message = "test_message"

        sut = BufferedBatch()
        sut.batch = {other_key: [message]}

        # Act and Assert
        self.assertEqual(0, sut.get_number_of_messages(key))
        self.assertEqual(1, sut.get_number_of_messages(other_key))

    def test_get_number_of_messages_with_empty_batch_and_used_buffer(self):
        # Arrange
        key = "test_key"
        other_key = "other_key"
        message = "test_message"

        sut = BufferedBatch()
        sut.buffer = {other_key: [message]}

        # Act and Assert
        self.assertEqual(0, sut.get_number_of_messages(key))
        self.assertEqual(0, sut.get_number_of_messages(other_key))

    def test_get_number_of_messages_with_multiple_keys_and_messages(self):
        # Arrange
        key_1 = "key_1"
        key_2 = "key_2"
        key_3 = "key_3"
        key_4 = "key_4"
        message_1 = "message_1"
        message_2 = "message_2"
        message_3 = "message_3"
        message_4 = "message_4"
        message_5 = "message_5"

        sut = BufferedBatch()
        sut.batch = {key_3: [message_1, message_5], key_1: [message_2], key_2: [message_3]}
        sut.buffer = {key_2: [message_4]}

        # Act and Assert
        self.assertEqual(1, sut.get_number_of_messages(key_1))
        self.assertEqual(1, sut.get_number_of_messages(key_2))
        self.assertEqual(2, sut.get_number_of_messages(key_3))
        self.assertEqual(0, sut.get_number_of_messages(key_4))


class TestGetNumberOfBufferedMessages(unittest.TestCase):
    def test_get_number_of_buffered_messages_with_empty_buffer(self):
        # Arrange
        key = "test_key"

        sut = BufferedBatch()

        # Act and Assert
        self.assertEqual(0, sut.get_number_of_buffered_messages(key))

    def test_get_number_of_buffered_messages_with_used_buffer_for_key(self):
        # Arrange
        key = "test_key"
        message = "test_message"

        sut = BufferedBatch()
        sut.buffer = {key: [message]}

        # Act and Assert
        self.assertEqual(1, sut.get_number_of_buffered_messages(key))

    def test_get_number_of_buffered_messages_with_used_buffer_for_other_key(self):
        # Arrange
        key = "test_key"
        other_key = "other_key"
        message = "test_message"

        sut = BufferedBatch()
        sut.buffer = {other_key: [message]}

        # Act and Assert
        self.assertEqual(0, sut.get_number_of_buffered_messages(key))
        self.assertEqual(1, sut.get_number_of_buffered_messages(other_key))

    def test_get_number_of_buffered_messages_with_empty_buffer_and_used_batch(self):
        # Arrange
        key = "test_key"
        other_key = "other_key"
        message = "test_message"

        sut = BufferedBatch()
        sut.batch = {other_key: [message]}

        # Act and Assert
        self.assertEqual(0, sut.get_number_of_buffered_messages(key))
        self.assertEqual(0, sut.get_number_of_buffered_messages(other_key))

    def test_get_number_of_buffered_messages_with_multiple_keys_and_messages(self):
        # Arrange
        key_1 = "key_1"
        key_2 = "key_2"
        key_3 = "key_3"
        key_4 = "key_4"
        message_1 = "message_1"
        message_2 = "message_2"
        message_3 = "message_3"
        message_4 = "message_4"
        message_5 = "message_5"

        sut = BufferedBatch()
        sut.buffer = {key_3: [message_1, message_5], key_1: [message_2], key_2: [message_3]}
        sut.batch = {key_2: [message_4]}

        # Act and Assert
        self.assertEqual(1, sut.get_number_of_buffered_messages(key_1))
        self.assertEqual(1, sut.get_number_of_buffered_messages(key_2))
        self.assertEqual(2, sut.get_number_of_buffered_messages(key_3))
        self.assertEqual(0, sut.get_number_of_buffered_messages(key_4))


class TestCompleteBatch(unittest.TestCase):
    def test_complete_batch_variant_1(self):
        def _convert_timestamp(timestamp: str):
            return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")

        def _add_seconds_to_timestamp(timestamp: datetime, seconds: float) -> datetime:
            delta = timedelta(seconds=seconds)
            return timestamp + delta

        # Arrange
        key = "test_key"
        message_1 = "message_1"
        message_2 = "message_2"
        expected_messages = [message_1, message_2]

        sut = BufferedBatch()

        # Act
        sut.add_message(key, message_1)
        time.sleep(0.2)  # simulate real behaviour of different message times
        sut.add_message(key, message_2)
        data = sut.complete_batch(key)

        # Assert
        self.assertGreaterEqual(
            _convert_timestamp(data["end_timestamp"]),
            _add_seconds_to_timestamp(_convert_timestamp(data["begin_timestamp"]), 0.2)
        )
        self.assertEqual(expected_messages, data["data"])

    def test_complete_batch_variant_2(self):
        def _convert_timestamp(timestamp: str):
            return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")

        def _add_seconds_to_timestamp(timestamp: datetime, seconds: float) -> datetime:
            delta = timedelta(seconds=seconds)
            return timestamp + delta

        # Arrange
        key = "test_key"
        message_1 = "message_1"
        message_2 = "message_2"
        message_3 = "message_3"
        message_4 = "message_4"

        sut = BufferedBatch()

        # Act
        sut.add_message(key, message_1)  # at +0.0 (begin_timestamp, data_1 and data_2)
        time.sleep(0.2)  # simulate real behaviour of different message times
        sut.add_message(key, message_2)  # at +0.2
        time.sleep(0.2)
        data_1 = sut.complete_batch(key)  # at +0.4 (end_timestamp, data_1)

        sut.add_message(key, message_3)  # at +0.4
        time.sleep(0.2)
        sut.add_message(key, message_4)  # at +0.6
        time.sleep(0.2)
        data_2 = sut.complete_batch(key)  # at +0.8 (end_timestamp, data_2)

        # Assert
        self.assertGreaterEqual(
            _convert_timestamp(data_1["end_timestamp"]),
            _add_seconds_to_timestamp(_convert_timestamp(data_1["begin_timestamp"]), 0.4)
        )
        self.assertGreaterEqual(
            _convert_timestamp(data_2["end_timestamp"]),
            _add_seconds_to_timestamp(_convert_timestamp(data_2["begin_timestamp"]), 0.8)
        )
        self.assertEqual(data_1["begin_timestamp"], data_2["begin_timestamp"])
        self.assertGreaterEqual(
            _convert_timestamp(data_2["end_timestamp"]),
            _add_seconds_to_timestamp(_convert_timestamp(data_1["end_timestamp"]), 0.4)
        )
        self.assertEqual({key: [message_3, message_4]}, sut.buffer)
        self.assertEqual({}, sut.batch)

    def test_complete_batch_variant_3(self):
        # Arrange
        key = "test_key"

        sut = BufferedBatch()
        sut.buffer = {key: ["message"]}

        # Act and Assert
        with self.assertRaises(ValueError):
            sut.complete_batch(key)

        self.assertEqual({}, sut.batch)
        self.assertEqual({}, sut.buffer, "Should have been emptied")

    def test_complete_batch_variant_4(self):
        # Arrange
        key = "test_key"

        sut = BufferedBatch()

        # Act and Assert
        with self.assertRaises(ValueError):
            sut.complete_batch(key)

        self.assertEqual({}, sut.batch)
        self.assertEqual({}, sut.buffer)


class TestGetStoredKeys(unittest.TestCase):
    def test_get_stored_keys_without_any_keys_stored(self):
        # Arrange
        sut = BufferedBatch()

        # Act and Assert
        self.assertEqual(set(), sut.get_stored_keys())

    def test_get_stored_keys_with_keys_stored_only_in_batch(self):
        # Arrange
        key_1 = "key_1"
        key_2 = "key_2"
        key_3 = "key_3"

        sut = BufferedBatch()
        sut.batch = {key_1: "message_1", key_2: "message_2", key_3: "message_3"}

        # Act and Assert
        self.assertEqual({key_1, key_2, key_3}, sut.get_stored_keys())

    def test_get_stored_keys_with_keys_stored_only_in_buffer(self):
        # Arrange
        key_1 = "key_1"
        key_2 = "key_2"
        key_3 = "key_3"

        sut = BufferedBatch()
        sut.buffer = {key_1: "message_1", key_2: "message_2", key_3: "message_3"}

        # Act and Assert
        self.assertEqual({key_1, key_2, key_3}, sut.get_stored_keys())

    def test_get_stored_keys_with_keys_stored_in_both_batch_and_buffer(self):
        # Arrange
        key_1 = "key_1"
        key_2 = "key_2"
        key_3 = "key_3"

        sut = BufferedBatch()
        sut.batch = {key_2: "message_2", key_3: "message_3"}
        sut.buffer = {key_1: "message_1"}

        # Act and Assert
        self.assertEqual({key_1, key_2, key_3}, sut.get_stored_keys())


if __name__ == '__main__':
    unittest.main()
