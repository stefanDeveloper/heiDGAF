import datetime
import unittest
import uuid
from unittest.mock import patch

from src.logcollector.batch_handler import BufferedBatch


class TestInit(unittest.TestCase):
    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_init(self, mock_clickhouse):
        # Act
        sut = BufferedBatch(collector_name="my-collector")

        # Assert
        self.assertEqual({}, sut.batch)
        self.assertEqual({}, sut.buffer)


class TestAddMessage(unittest.TestCase):
    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_add_message_empty_batch_and_empty_buffer(self, mock_clickhouse):
        # Arrange
        key = "test_key"
        message = "test_message"

        sut = BufferedBatch(collector_name = "test_collector")

        # Act
        sut.add_message(key, uuid.uuid4(), message)

        # Assert
        self.assertEqual(
            {key: [message]}, sut.batch, "Batch should contain the key with the message"
        )
        self.assertEqual({}, sut.buffer, "Buffer should remain empty")

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_add_message_empty_batch_and_used_buffer(self, mock_clickhouse):
        # Arrange
        key = "test_key"
        message = "test_message"
        old_message = "old_message"

        sut = BufferedBatch(collector_name = "test_collector")
        sut.buffer = {key: [old_message]}

        # Act
        sut.add_message(key, uuid.uuid4(), message)

        # Assert
        self.assertEqual(
            {key: [message]}, sut.batch, "Batch should contain the key with the message"
        )
        self.assertEqual(
            {key: [old_message]},
            sut.buffer,
            "Buffer should still contain key with old message",
        )

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_add_message_used_batch_and_empty_buffer(self, mock_clickhouse):
        # Arrange
        key = "test_key"
        message = "test_message"
        old_message = "old_message"

        sut = BufferedBatch(collector_name = "test_collector")
        sut.batch = {key: [old_message]}

        # Act
        sut.add_message(key, uuid.uuid4(), message)

        # Assert
        self.assertEqual(
            {key: [old_message, message]},
            sut.batch,
            "Batch should contain the key with the old and new message",
        )
        self.assertEqual({}, sut.buffer, "Buffer should remain empty")

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_add_message_used_batch_and_used_buffer(self, mock_clickhouse):
        # Arrange
        key = "test_key"
        message = "test_message"
        old_message_1 = "old_message_1"
        old_message_2 = "old_message_2"

        sut = BufferedBatch(collector_name = "test_collector")
        sut.batch = {key: [old_message_2]}
        sut.buffer = {key: [old_message_1]}

        # Act
        sut.add_message(key, uuid.uuid4(), message)

        # Assert
        self.assertEqual(
            {key: [old_message_2, message]},
            sut.batch,
            "Batch should contain the key with the old and new message",
        )
        self.assertEqual(
            {key: [old_message_1]},
            sut.buffer,
            "Buffer should still contain key with old message",
        )

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_add_message_with_existing_other_key(self, mock_clickhouse):
        # Arrange
        key = "test_key"
        message = "test_message"
        old_key = "old_key"
        old_message_1 = "old_message_1"
        old_message_2 = "old_message_2"

        sut = BufferedBatch(collector_name = "test_collector")
        sut.batch = {old_key: [old_message_2]}
        sut.buffer = {old_key: [old_message_1]}

        # Act
        sut.add_message(key, uuid.uuid4(), message)

        # Assert
        self.assertEqual(
            {old_key: [old_message_2], key: [message]},
            sut.batch,
            "Batch should contain the old key with the old message and the new key with the new message",
        )
        self.assertEqual(
            {old_key: [old_message_1]},
            sut.buffer,
            "Buffer should still contain old key with old message",
        )


class TestGetNumberOfMessages(unittest.TestCase):
    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_get_number_of_messages_with_empty_batch(self, mock_clickhouse):
        # Arrange
        key = "test_key"

        sut = BufferedBatch(collector_name = "test_collector")

        # Act and Assert
        self.assertEqual(0, sut.get_message_count_for_batch_key(key))

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_get_number_of_messages_with_used_batch_for_key(self, mock_clickhouse):
        # Arrange
        key = "test_key"
        message = "test_message"

        sut = BufferedBatch(collector_name = "test_collector")
        sut.batch = {key: [message]}

        # Act and Assert
        self.assertEqual(1, sut.get_message_count_for_batch_key(key))

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_get_number_of_messages_with_used_batch_for_other_key(
        self, mock_clickhouse
    ):
        # Arrange
        key = "test_key"
        other_key = "other_key"
        message = "test_message"

        sut = BufferedBatch(collector_name = "test_collector")
        sut.batch = {other_key: [message]}

        # Act and Assert
        self.assertEqual(0, sut.get_message_count_for_batch_key(key))
        self.assertEqual(1, sut.get_message_count_for_batch_key(other_key))

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_get_number_of_messages_with_empty_batch_and_used_buffer(
        self, mock_clickhouse
    ):
        # Arrange
        key = "test_key"
        other_key = "other_key"
        message = "test_message"

        sut = BufferedBatch(collector_name = "test_collector")
        sut.buffer = {other_key: [message]}

        # Act and Assert
        self.assertEqual(0, sut.get_message_count_for_batch_key(key))
        self.assertEqual(0, sut.get_message_count_for_batch_key(other_key))

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_get_number_of_messages_with_multiple_keys_and_messages(
        self, mock_clickhouse
    ):
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

        sut = BufferedBatch(collector_name = "test_collector")
        sut.batch = {
            key_3: [message_1, message_5],
            key_1: [message_2],
            key_2: [message_3],
        }
        sut.buffer = {key_2: [message_4]}

        # Act and Assert
        self.assertEqual(1, sut.get_message_count_for_batch_key(key_1))
        self.assertEqual(1, sut.get_message_count_for_batch_key(key_2))
        self.assertEqual(2, sut.get_message_count_for_batch_key(key_3))
        self.assertEqual(0, sut.get_message_count_for_batch_key(key_4))


class TestGetNumberOfBufferedMessages(unittest.TestCase):
    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_get_number_of_buffered_messages_with_empty_buffer(self, mock_clickhouse):
        # Arrange
        key = "test_key"

        sut = BufferedBatch(collector_name = "test_collector")

        # Act and Assert
        self.assertEqual(0, sut.get_message_count_for_buffer_key(key))

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_get_number_of_buffered_messages_with_used_buffer_for_key(
        self, mock_clickhouse
    ):
        # Arrange
        key = "test_key"
        message = "test_message"

        sut = BufferedBatch(collector_name = "test_collector")
        sut.buffer = {key: [message]}

        # Act and Assert
        self.assertEqual(1, sut.get_message_count_for_buffer_key(key))

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_get_number_of_buffered_messages_with_used_buffer_for_other_key(
        self, mock_clickhouse
    ):
        # Arrange
        key = "test_key"
        other_key = "other_key"
        message = "test_message"

        sut = BufferedBatch(collector_name = "test_collector")
        sut.buffer = {other_key: [message]}

        # Act and Assert
        self.assertEqual(0, sut.get_message_count_for_buffer_key(key))
        self.assertEqual(1, sut.get_message_count_for_buffer_key(other_key))

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_get_number_of_buffered_messages_with_empty_buffer_and_used_batch(
        self, mock_clickhouse
    ):
        # Arrange
        key = "test_key"
        other_key = "other_key"
        message = "test_message"

        sut = BufferedBatch(collector_name = "test_collector")
        sut.batch = {other_key: [message]}

        # Act and Assert
        self.assertEqual(0, sut.get_message_count_for_buffer_key(key))
        self.assertEqual(0, sut.get_message_count_for_buffer_key(other_key))

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_get_number_of_buffered_messages_with_multiple_keys_and_messages(
        self, mock_clickhouse
    ):
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

        sut = BufferedBatch(collector_name = "test_collector")
        sut.buffer = {
            key_3: [message_1, message_5],
            key_1: [message_2],
            key_2: [message_3],
        }
        sut.batch = {key_2: [message_4]}

        # Act and Assert
        self.assertEqual(1, sut.get_message_count_for_buffer_key(key_1))
        self.assertEqual(1, sut.get_message_count_for_buffer_key(key_2))
        self.assertEqual(2, sut.get_message_count_for_buffer_key(key_3))
        self.assertEqual(0, sut.get_message_count_for_buffer_key(key_4))


class TestSortMessages(unittest.TestCase):
    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_sort_with_empty_list(self, mock_clickhouse):
        # Arrange
        list_of_timestamps_and_loglines = []
        sut = BufferedBatch(collector_name = "test_collector")

        # Act
        result = sut._sort_by_timestamp(list_of_timestamps_and_loglines)

        # Assert
        self.assertEqual([], result)

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_sort_with_sorted_list(self, mock_clickhouse):
        # Arrange
        list_of_timestamps_and_loglines = [
            (
                "2024-05-21T08:31:28.119Z",
                '{"ts": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "src_ip": '
                '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
                '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
                '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            ),
            (
                "2024-05-21T08:31:28.249Z",
                '{"ts": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "src_ip": '
                '"192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": '
                '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
                '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}',
            ),
        ]
        expected_list = [
            '{"ts": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "src_ip": '
            '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            '{"ts": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "src_ip": '
            '"192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}',
        ]
        sut = BufferedBatch(collector_name = "test_collector")

        # Act
        result = sut._sort_by_timestamp(list_of_timestamps_and_loglines)

        # Assert
        self.assertEqual(expected_list, result)

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_sort_with_unsorted_list(self, mock_clickhouse):
        # Arrange
        list_of_timestamps_and_loglines = [
            (
                "2024-05-21T08:31:28.249Z",
                '{"ts": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "src_ip": '
                '"192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": '
                '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
                '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}',
            ),
            (
                "2024-05-21T08:31:28.119Z",
                '{"ts": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "src_ip": '
                '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
                '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
                '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            ),
        ]
        expected_list = [
            '{"ts": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "src_ip": '
            '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            '{"ts": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "src_ip": '
            '"192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}',
        ]
        sut = BufferedBatch(collector_name="my-collector")

        # Act
        result = sut._sort_by_timestamp(list_of_timestamps_and_loglines)

        # Assert
        self.assertEqual(expected_list, result)


class TestExtractTuplesFromJson(unittest.TestCase):
    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_empty_data(self, mock_clickhouse):
        # Arrange
        sut = BufferedBatch(collector_name = "test_collector")
        data = []

        # Act
        result = sut._extract_tuples_from_json_formatted_strings(data)

        # Assert
        self.assertEqual([], result)

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_with_data(self, mock_clickhouse):
        # Arrange
        sut = BufferedBatch(collector_name = "test_collector")
        data = [
            '{"ts": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "src_ip": '
            '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            '{"ts": "2024-05-21T08:31:28.299Z", "status": "NXDOMAIN", "src_ip": '
            '"192.168.0.106", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
        ]
        expected_result = [
            (
                "2024-05-21T08:31:28.119Z",
                '{"ts": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "src_ip": '
                '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
                '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
                '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            ),
            (
                "2024-05-21T08:31:28.299Z",
                '{"ts": "2024-05-21T08:31:28.299Z", "status": "NXDOMAIN", "src_ip": '
                '"192.168.0.106", "dns_ip": "8.8.8.8", "host_domain_name": '
                '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
                '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            ),
        ]

        # Act
        result = sut._extract_tuples_from_json_formatted_strings(data)

        # Assert
        self.assertEqual(expected_result, result)


class TestSortBuffer(unittest.TestCase):
    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_sort_empty_buffer(self, mock_clickhouse):
        # Arrange
        key = "test_key"
        sut = BufferedBatch(collector_name = "test_collector")
        sut.buffer = []

        # Act
        sut._sort_buffer(key)

        # Assert
        self.assertEqual([], sut.buffer)

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_sort_sorted_buffer(self, mock_clickhouse):
        # Arrange
        key = "test_key"
        sut = BufferedBatch(collector_name = "test_collector")
        sut.buffer[key] = [
            '{"ts": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "src_ip": '
            '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            '{"ts": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "src_ip": '
            '"192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}',
            '{"ts": "2024-05-21T08:31:28.378Z", "status": "NXDOMAIN", "src_ip": '
            '"192.168.0.221", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "120b"}',
        ]
        expected_buffer = sut.buffer[key].copy()

        # Act
        sut._sort_buffer(key)

        # Assert
        self.assertEqual(expected_buffer, sut.buffer[key])

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_sort_unsorted_buffer(self, mock_clickhouse):
        # Arrange
        key = "test_key"
        sut = BufferedBatch(collector_name = "test_collector")
        sut.buffer[key] = [
            '{"ts": "2024-05-21T08:31:28.378Z", "status": "NXDOMAIN", "src_ip": '
            '"192.168.0.221", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "120b"}',
            '{"ts": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "src_ip": '
            '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            '{"ts": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "src_ip": '
            '"192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}',
        ]
        expected_buffer = [
            '{"ts": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "src_ip": '
            '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            '{"ts": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "src_ip": '
            '"192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}',
            '{"ts": "2024-05-21T08:31:28.378Z", "status": "NXDOMAIN", "src_ip": '
            '"192.168.0.221", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "120b"}',
        ]

        # Act
        sut._sort_buffer(key)

        # Assert
        self.assertEqual(expected_buffer, sut.buffer[key])


class TestSortBatch(unittest.TestCase):
    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_sort_empty_batch(self, mock_clickhouse):
        # Arrange
        key = "test_key"
        sut = BufferedBatch(collector_name = "test_collector")
        sut.batch = []

        # Act
        sut._sort_batch(key)

        # Assert
        self.assertEqual([], sut.batch)

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_sort_sorted_batch(self, mock_clickhouse):
        # Arrange
        key = "test_key"
        sut = BufferedBatch(collector_name = "test_collector")
        sut.batch[key] = [
            '{"ts": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "src_ip": '
            '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            '{"ts": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "src_ip": '
            '"192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}',
            '{"ts": "2024-05-21T08:31:28.378Z", "status": "NXDOMAIN", "src_ip": '
            '"192.168.0.221", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "120b"}',
        ]
        expected_batch = sut.batch[key].copy()

        # Act
        sut._sort_batch(key)

        # Assert
        self.assertEqual(expected_batch, sut.batch[key])

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_sort_unsorted_buffer(self, mock_clickhouse):
        # Arrange
        key = "test_key"
        sut = BufferedBatch(collector_name = "test_collector")
        sut.batch[key] = [
            '{"ts": "2024-05-21T08:31:28.378Z", "status": "NXDOMAIN", "src_ip": '
            '"192.168.0.221", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "120b"}',
            '{"ts": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "src_ip": '
            '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            '{"ts": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "src_ip": '
            '"192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}',
        ]
        expected_batch = [
            '{"ts": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "src_ip": '
            '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            '{"ts": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "src_ip": '
            '"192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}',
            '{"ts": "2024-05-21T08:31:28.378Z", "status": "NXDOMAIN", "src_ip": '
            '"192.168.0.221", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "120b"}',
        ]

        # Act
        sut._sort_batch(key)

        # Assert
        self.assertEqual(expected_batch, sut.batch[key])


class TestCompleteBatch(unittest.TestCase):
    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_complete_batch_variant_1(self, mock_clickhouse):
        # Arrange
        key = "test_key"
        message_1 = (
            '{"ts": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "src_ip": "192.168.0.105", '
            '"dns_ip": "8.8.8.8", "host_domain_name": "www.heidelberg-botanik.de", "record_type": "A", '
            '"response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}'
        )
        message_2 = (
            '{"ts": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "src_ip": "192.168.0.230", '
            '"dns_ip": "8.8.8.8", "host_domain_name": "www.heidelberg-botanik.de", "record_type": "AAAA", '
            '"response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}'
        )

        expected_messages = [message_1, message_2]

        sut = BufferedBatch(collector_name = "test_collector")

        # Act
        sut.add_message(key, uuid.uuid4(), message_2)
        sut.add_message(key, uuid.uuid4(), message_1)
        data = sut.complete_batch(key)

        # Assert
        self.assertEqual(
            datetime.datetime(
                2024, 5, 21, 8, 31, 28, 119000, tzinfo=datetime.timezone.utc
            ),
            data["begin_timestamp"],
        )
        self.assertEqual(
            datetime.datetime(
                2024, 5, 21, 8, 31, 28, 249000, tzinfo=datetime.timezone.utc
            ),
            data["end_timestamp"],
        )
        self.assertEqual(expected_messages, data["data"])

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_complete_batch_variant_2(self, mock_clickhouse):
        # Arrange
        key = "test_key"
        message_1 = (
            '{"ts": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "src_ip": "192.168.0.105", '
            '"dns_ip": "8.8.8.8", "host_domain_name": "www.heidelberg-botanik.de", "record_type": "A", '
            '"response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}'
        )
        message_2 = (
            '{"ts": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "src_ip": "192.168.0.230", '
            '"dns_ip": "8.8.8.8", "host_domain_name": "www.heidelberg-botanik.de", "record_type": "AAAA", '
            '"response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}'
        )
        message_3 = (
            '{"ts": "2024-05-21T08:31:28.319Z", "status": "NOERROR", "src_ip": "192.168.0.105", '
            '"dns_ip": "8.8.8.8", "host_domain_name": "www.heidelberg-botanik.de", "record_type": "A", '
            '"response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}'
        )
        message_4 = (
            '{"ts": "2024-05-21T08:31:28.749Z", "status": "NXDOMAIN", "src_ip": "192.168.0.230", '
            '"dns_ip": "8.8.8.8", "host_domain_name": "www.heidelberg-botanik.de", "record_type": "AAAA", '
            '"response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}'
        )

        sut = BufferedBatch(collector_name = "test_collector")

        # Act
        sut.add_message(key, uuid.uuid4(), message_1)
        sut.add_message(key, uuid.uuid4(), message_2)
        data_1 = sut.complete_batch(key)

        sut.add_message(key, uuid.uuid4(), message_3)
        sut.add_message(key, uuid.uuid4(), message_4)
        data_2 = sut.complete_batch(key)

        # Assert
        self.assertEqual(
            datetime.datetime(
                2024, 5, 21, 8, 31, 28, 119000, tzinfo=datetime.timezone.utc
            ),
            data_1["begin_timestamp"],
        )
        self.assertEqual(
            datetime.datetime(
                2024, 5, 21, 8, 31, 28, 249000, tzinfo=datetime.timezone.utc
            ),
            data_1["end_timestamp"],
        )
        self.assertEqual(
            datetime.datetime(
                2024, 5, 21, 8, 31, 28, 119000, tzinfo=datetime.timezone.utc
            ),
            data_2["begin_timestamp"],
        )
        self.assertEqual(
            datetime.datetime(
                2024, 5, 21, 8, 31, 28, 749000, tzinfo=datetime.timezone.utc
            ),
            data_2["end_timestamp"],
        )
        self.assertEqual({key: [message_3, message_4]}, sut.buffer)
        self.assertEqual({}, sut.batch)

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_complete_batch_variant_3(self, mock_clickhouse):
        # Arrange
        key = "test_key"

        sut = BufferedBatch(collector_name = "test_collector")
        sut.buffer = {key: ["message"]}

        # Act and Assert
        with self.assertRaises(ValueError):
            sut.complete_batch(key)

        self.assertEqual({}, sut.batch)
        self.assertEqual({}, sut.buffer, "Should have been emptied")

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_complete_batch_variant_4(self, mock_clickhouse):
        # Arrange
        key = "test_key"

        sut = BufferedBatch(collector_name = "test_collector")

        # Act and Assert
        with self.assertRaises(ValueError):
            sut.complete_batch(key)

        self.assertEqual({}, sut.batch)
        self.assertEqual({}, sut.buffer)


class TestGetStoredKeys(unittest.TestCase):
    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_get_stored_keys_without_any_keys_stored(self, mock_clickhouse):
        # Arrange
        sut = BufferedBatch(collector_name = "test_collector")

        # Act and Assert
        self.assertEqual(set(), sut.get_stored_keys())

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_get_stored_keys_with_keys_stored_only_in_batch(self, mock_clickhouse):
        # Arrange
        key_1 = "key_1"
        key_2 = "key_2"
        key_3 = "key_3"

        sut = BufferedBatch(collector_name = "test_collector")
        sut.batch = {key_1: "message_1", key_2: "message_2", key_3: "message_3"}

        # Act and Assert
        self.assertEqual({key_1, key_2, key_3}, sut.get_stored_keys())

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_get_stored_keys_with_keys_stored_only_in_buffer(self, mock_clickhouse):
        # Arrange
        key_1 = "key_1"
        key_2 = "key_2"
        key_3 = "key_3"

        sut = BufferedBatch(collector_name = "test_collector")
        sut.buffer = {key_1: "message_1", key_2: "message_2", key_3: "message_3"}

        # Act and Assert
        self.assertEqual({key_1, key_2, key_3}, sut.get_stored_keys())

    @patch("src.logcollector.batch_handler.ClickHouseKafkaSender")
    def test_get_stored_keys_with_keys_stored_in_both_batch_and_buffer(
        self, mock_clickhouse
    ):
        # Arrange
        key_1 = "key_1"
        key_2 = "key_2"
        key_3 = "key_3"

        sut = BufferedBatch(collector_name = "test_collector")
        sut.batch = {key_2: "message_2", key_3: "message_3"}
        sut.buffer = {key_1: "message_1"}

        # Act and Assert
        self.assertEqual({key_1, key_2, key_3}, sut.get_stored_keys())


if __name__ == "__main__":
    unittest.main()
