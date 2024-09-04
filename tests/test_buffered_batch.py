import unittest

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


class TestSortMessages(unittest.TestCase):
    def test_sort_with_empty_list(self):
        # Arrange
        list_of_timestamps_and_loglines = []
        sut = BufferedBatch()

        # Act
        result = sut.sort_messages(list_of_timestamps_and_loglines)

        # Assert
        self.assertEqual([], result)

    def test_sort_with_sorted_list(self):
        # Arrange
        list_of_timestamps_and_loglines = [
            ('2024-05-21T08:31:28.119Z', '{"timestamp": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": '
                                         '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
                                         '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
                                         '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}'),
            ('2024-05-21T08:31:28.249Z', '{"timestamp": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "client_ip": '
                                         '"192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": '
                                         '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
                                         '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}')
        ]
        expected_list = [
            '{"timestamp": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": '
            '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            '{"timestamp": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "client_ip": '
            '"192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}'
        ]
        sut = BufferedBatch()

        # Act
        result = sut.sort_messages(list_of_timestamps_and_loglines)

        # Assert
        self.assertEqual(expected_list, result)

    def test_sort_with_unsorted_list(self):
        # Arrange
        list_of_timestamps_and_loglines = [
            ('2024-05-21T08:31:28.249Z', '{"timestamp": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "client_ip": '
                                         '"192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": '
                                         '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
                                         '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}'),
            ('2024-05-21T08:31:28.119Z', '{"timestamp": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": '
                                         '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
                                         '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
                                         '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}')
        ]
        expected_list = [
            '{"timestamp": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": '
            '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            '{"timestamp": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "client_ip": '
            '"192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}'
        ]
        sut = BufferedBatch()

        # Act
        result = sut.sort_messages(list_of_timestamps_and_loglines)

        # Assert
        self.assertEqual(expected_list, result)


class TestExtractTuplesFromJson(unittest.TestCase):
    def test_empty_data(self):
        # Arrange
        sut = BufferedBatch()
        data = []

        # Act
        result = sut.extract_tuples_from_json_formatted_strings(data)

        # Assert
        self.assertEqual([], result)

    def test_with_data(self):
        # Arrange
        sut = BufferedBatch()
        data = [
            '{"timestamp": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": '
            '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            '{"timestamp": "2024-05-21T08:31:28.299Z", "status": "NXDOMAIN", "client_ip": '
            '"192.168.0.106", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}'
        ]
        expected_result = [
            ("2024-05-21T08:31:28.119Z", '{"timestamp": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": '
                                         '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
                                         '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
                                         '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}'),
            ("2024-05-21T08:31:28.299Z", '{"timestamp": "2024-05-21T08:31:28.299Z", "status": "NXDOMAIN", "client_ip": '
                                         '"192.168.0.106", "dns_ip": "8.8.8.8", "host_domain_name": '
                                         '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
                                         '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}')
        ]

        # Act
        result = sut.extract_tuples_from_json_formatted_strings(data)

        # Assert
        self.assertEqual(expected_result, result)


class TestGetFirstTimestampOfBuffer(unittest.TestCase):
    def test_with_data(self):
        # Arrange
        key = "test_key"
        sut = BufferedBatch()
        sut.buffer[key] = [
            '{"timestamp": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": "www.test.de", "record_type": "A", "response_ip": "fe80::1", "size": "150b"}',
            '{"timestamp": "2024-01-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.106", "dns_ip": "8.8.8.8", "host_domain_name": "www.example.com", "record_type": "A", "response_ip": "fe80::2", "size": "200b"}',
            '{"timestamp": "2024-12-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.107", "dns_ip": "8.8.8.8", "host_domain_name": "www.sample.com", "record_type": "A", "response_ip": "fe80::3", "size": "250b"}'
        ]
        sut.batch[key] = [
            '{"timestamp": "2025-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.108", "dns_ip": "8.8.8.8", "host_domain_name": "www.example2.com", "record_type": "A", "response_ip": "fe80::4", "size": "300b"}',
            '{"timestamp": "2025-01-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.109", "dns_ip": "8.8.8.8", "host_domain_name": "www.test2.de", "record_type": "A", "response_ip": "fe80::5", "size": "350b"}',
            '{"timestamp": "2025-12-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.110", "dns_ip": "8.8.8.8", "host_domain_name": "www.sample2.com", "record_type": "A", "response_ip": "fe80::6", "size": "400b"}'
        ]

        # Act
        result = sut.get_first_timestamp_of_buffer(key)

        # Assert
        self.assertEqual("2024-05-21T08:31:28.119Z", result)

    def test_no_data(self):
        # Arrange
        key = "test_key"
        sut = BufferedBatch()
        sut.buffer[key] = []
        sut.batch[key] = []

        # Act
        result = sut.get_first_timestamp_of_buffer(key)

        # Assert
        self.assertIsNone(result)

    def test_key_does_not_exist(self):
        # Arrange
        key = "test_key"
        sut = BufferedBatch()

        # Act
        result = sut.get_first_timestamp_of_buffer(key)

        # Assert
        self.assertIsNone(result)


class TestGetFirstTimestampOfBatch(unittest.TestCase):
    def test_with_data(self):
        # Arrange
        key = "test_key"
        sut = BufferedBatch()
        sut.buffer[key] = [
            '{"timestamp": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": "www.test.de", "record_type": "A", "response_ip": "fe80::1", "size": "150b"}',
            '{"timestamp": "2024-01-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.106", "dns_ip": "8.8.8.8", "host_domain_name": "www.example.com", "record_type": "A", "response_ip": "fe80::2", "size": "200b"}',
            '{"timestamp": "2024-12-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.107", "dns_ip": "8.8.8.8", "host_domain_name": "www.sample.com", "record_type": "A", "response_ip": "fe80::3", "size": "250b"}'
        ]
        sut.batch[key] = [
            '{"timestamp": "2025-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.108", "dns_ip": "8.8.8.8", "host_domain_name": "www.example2.com", "record_type": "A", "response_ip": "fe80::4", "size": "300b"}',
            '{"timestamp": "2025-01-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.109", "dns_ip": "8.8.8.8", "host_domain_name": "www.test2.de", "record_type": "A", "response_ip": "fe80::5", "size": "350b"}',
            '{"timestamp": "2025-12-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.110", "dns_ip": "8.8.8.8", "host_domain_name": "www.sample2.com", "record_type": "A", "response_ip": "fe80::6", "size": "400b"}'
        ]

        # Act
        result = sut.get_first_timestamp_of_batch(key)

        # Assert
        self.assertEqual("2025-05-21T08:31:28.119Z", result)

    def test_no_data(self):
        # Arrange
        key = "test_key"
        sut = BufferedBatch()
        sut.buffer[key] = []
        sut.batch[key] = []

        # Act
        result = sut.get_first_timestamp_of_batch(key)

        # Assert
        self.assertIsNone(result)

    def test_key_does_not_exist(self):
        # Arrange
        key = "test_key"
        sut = BufferedBatch()

        # Act
        result = sut.get_first_timestamp_of_batch(key)

        # Assert
        self.assertIsNone(result)


class TestGetLastTimestampOfBatch(unittest.TestCase):
    def test_with_data(self):
        # Arrange
        key = "test_key"
        sut = BufferedBatch()
        sut.buffer[key] = [
            '{"timestamp": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": "www.test.de", "record_type": "A", "response_ip": "fe80::1", "size": "150b"}',
            '{"timestamp": "2024-01-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.106", "dns_ip": "8.8.8.8", "host_domain_name": "www.example.com", "record_type": "A", "response_ip": "fe80::2", "size": "200b"}',
            '{"timestamp": "2024-12-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.107", "dns_ip": "8.8.8.8", "host_domain_name": "www.sample.com", "record_type": "A", "response_ip": "fe80::3", "size": "250b"}'
        ]
        sut.batch[key] = [
            '{"timestamp": "2025-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.108", "dns_ip": "8.8.8.8", "host_domain_name": "www.example2.com", "record_type": "A", "response_ip": "fe80::4", "size": "300b"}',
            '{"timestamp": "2025-01-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.109", "dns_ip": "8.8.8.8", "host_domain_name": "www.test2.de", "record_type": "A", "response_ip": "fe80::5", "size": "350b"}',
            '{"timestamp": "2025-12-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.110", "dns_ip": "8.8.8.8", "host_domain_name": "www.sample2.com", "record_type": "A", "response_ip": "fe80::6", "size": "400b"}'
        ]

        # Act
        result = sut.get_last_timestamp_of_batch(key)

        # Assert
        self.assertEqual("2025-12-21T08:31:28.119Z", result)

    def test_no_data(self):
        # Arrange
        key = "test_key"
        sut = BufferedBatch()
        sut.buffer[key] = []
        sut.batch[key] = []

        # Act
        result = sut.get_last_timestamp_of_batch(key)

        # Assert
        self.assertIsNone(result)

    def test_key_does_not_exist(self):
        # Arrange
        key = "test_key"
        sut = BufferedBatch()

        # Act
        result = sut.get_last_timestamp_of_batch(key)

        # Assert
        self.assertIsNone(result)


class TestGetLastTimestampOfBuffer(unittest.TestCase):
    def test_with_data(self):
        # Arrange
        key = "test_key"
        sut = BufferedBatch()
        sut.buffer[key] = [
            '{"timestamp": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": "www.test.de", "record_type": "A", "response_ip": "fe80::1", "size": "150b"}',
            '{"timestamp": "2024-01-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.106", "dns_ip": "8.8.8.8", "host_domain_name": "www.example.com", "record_type": "A", "response_ip": "fe80::2", "size": "200b"}',
            '{"timestamp": "2024-12-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.107", "dns_ip": "8.8.8.8", "host_domain_name": "www.sample.com", "record_type": "A", "response_ip": "fe80::3", "size": "250b"}'
        ]
        sut.batch[key] = [
            '{"timestamp": "2025-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.108", "dns_ip": "8.8.8.8", "host_domain_name": "www.example2.com", "record_type": "A", "response_ip": "fe80::4", "size": "300b"}',
            '{"timestamp": "2025-01-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.109", "dns_ip": "8.8.8.8", "host_domain_name": "www.test2.de", "record_type": "A", "response_ip": "fe80::5", "size": "350b"}',
            '{"timestamp": "2025-12-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.110", "dns_ip": "8.8.8.8", "host_domain_name": "www.sample2.com", "record_type": "A", "response_ip": "fe80::6", "size": "400b"}'
        ]

        # Act
        result = sut.get_last_timestamp_of_buffer(key)

        # Assert
        self.assertEqual("2024-12-21T08:31:28.119Z", result)

    def test_no_data(self):
        # Arrange
        key = "test_key"
        sut = BufferedBatch()
        sut.buffer[key] = []
        sut.batch[key] = []

        # Act
        result = sut.get_last_timestamp_of_buffer(key)

        # Assert
        self.assertIsNone(result)

    def test_key_does_not_exist(self):
        # Arrange
        key = "test_key"
        sut = BufferedBatch()

        # Act
        result = sut.get_last_timestamp_of_buffer(key)

        # Assert
        self.assertIsNone(result)


class TestSortBuffer(unittest.TestCase):
    def test_sort_empty_buffer(self):
        # Arrange
        key = "test_key"
        sut = BufferedBatch()
        sut.buffer = []

        # Act
        sut.sort_buffer(key)

        # Assert
        self.assertEqual([], sut.buffer)

    def test_sort_sorted_buffer(self):
        # Arrange
        key = "test_key"
        sut = BufferedBatch()
        sut.buffer[key] = [
            '{"timestamp": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": '
            '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            '{"timestamp": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "client_ip": '
            '"192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}',
            '{"timestamp": "2024-05-21T08:31:28.378Z", "status": "NXDOMAIN", "client_ip": '
            '"192.168.0.221", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "120b"}'
        ]
        expected_buffer = sut.buffer[key].copy()

        # Act
        sut.sort_buffer(key)

        # Assert
        self.assertEqual(expected_buffer, sut.buffer[key])

    def test_sort_unsorted_buffer(self):
        # Arrange
        key = "test_key"
        sut = BufferedBatch()
        sut.buffer[key] = [
            '{"timestamp": "2024-05-21T08:31:28.378Z", "status": "NXDOMAIN", "client_ip": '
            '"192.168.0.221", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "120b"}',
            '{"timestamp": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": '
            '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            '{"timestamp": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "client_ip": '
            '"192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}'

        ]
        expected_buffer = [
            '{"timestamp": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": '
            '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            '{"timestamp": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "client_ip": '
            '"192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}',
            '{"timestamp": "2024-05-21T08:31:28.378Z", "status": "NXDOMAIN", "client_ip": '
            '"192.168.0.221", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "120b"}'
        ]

        # Act
        sut.sort_buffer(key)

        # Assert
        self.assertEqual(expected_buffer, sut.buffer[key])


class TestSortBatch(unittest.TestCase):
    def test_sort_empty_batch(self):
        # Arrange
        key = "test_key"
        sut = BufferedBatch()
        sut.batch = []

        # Act
        sut.sort_batch(key)

        # Assert
        self.assertEqual([], sut.batch)

    def test_sort_sorted_batch(self):
        # Arrange
        key = "test_key"
        sut = BufferedBatch()
        sut.batch[key] = [
            '{"timestamp": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": '
            '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            '{"timestamp": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "client_ip": '
            '"192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}',
            '{"timestamp": "2024-05-21T08:31:28.378Z", "status": "NXDOMAIN", "client_ip": '
            '"192.168.0.221", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "120b"}'
        ]
        expected_batch = sut.batch[key].copy()

        # Act
        sut.sort_batch(key)

        # Assert
        self.assertEqual(expected_batch, sut.batch[key])

    def test_sort_unsorted_buffer(self):
        # Arrange
        key = "test_key"
        sut = BufferedBatch()
        sut.batch[key] = [
            '{"timestamp": "2024-05-21T08:31:28.378Z", "status": "NXDOMAIN", "client_ip": '
            '"192.168.0.221", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "120b"}',
            '{"timestamp": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": '
            '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            '{"timestamp": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "client_ip": '
            '"192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}'

        ]
        expected_batch = [
            '{"timestamp": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": '
            '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}',
            '{"timestamp": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "client_ip": '
            '"192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}',
            '{"timestamp": "2024-05-21T08:31:28.378Z", "status": "NXDOMAIN", "client_ip": '
            '"192.168.0.221", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "120b"}'
        ]

        # Act
        sut.sort_batch(key)

        # Assert
        self.assertEqual(expected_batch, sut.batch[key])


class TestCompleteBatch(unittest.TestCase):
    def test_complete_batch_variant_1(self):
        # Arrange
        key = "test_key"
        message_1 = '{"timestamp": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": "www.heidelberg-botanik.de", "record_type": "A", "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}'
        message_2 = '{"timestamp": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "client_ip": "192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": "www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}'

        expected_messages = [message_1, message_2]

        sut = BufferedBatch()

        # Act
        sut.add_message(key, message_2)
        sut.add_message(key, message_1)
        data = sut.complete_batch(key)

        # Assert
        self.assertEqual("2024-05-21T08:31:28.119Z", data["begin_timestamp"])
        self.assertEqual("2024-05-21T08:31:28.249Z", data["end_timestamp"])
        self.assertEqual(expected_messages, data["data"])

    def test_complete_batch_variant_2(self):
        # Arrange
        key = "test_key"
        message_1 = '{"timestamp": "2024-05-21T08:31:28.119Z", "status": "NOERROR", "client_ip": "192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": "www.heidelberg-botanik.de", "record_type": "A", "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}'
        message_2 = '{"timestamp": "2024-05-21T08:31:28.249Z", "status": "NXDOMAIN", "client_ip": "192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": "www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}'
        message_3 = '{"timestamp": "2024-05-21T08:31:28.319Z", "status": "NOERROR", "client_ip": "192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": "www.heidelberg-botanik.de", "record_type": "A", "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}'
        message_4 = '{"timestamp": "2024-05-21T08:31:28.749Z", "status": "NXDOMAIN", "client_ip": "192.168.0.230", "dns_ip": "8.8.8.8", "host_domain_name": "www.heidelberg-botanik.de", "record_type": "AAAA", "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "100b"}'

        sut = BufferedBatch()

        # Act
        sut.add_message(key, message_1)
        sut.add_message(key, message_2)
        data_1 = sut.complete_batch(key)

        sut.add_message(key, message_3)
        sut.add_message(key, message_4)
        data_2 = sut.complete_batch(key)

        # Assert
        self.assertEqual("2024-05-21T08:31:28.119Z", data_1["begin_timestamp"])
        self.assertEqual("2024-05-21T08:31:28.249Z", data_1["end_timestamp"])
        self.assertEqual("2024-05-21T08:31:28.119Z", data_2["begin_timestamp"])
        self.assertEqual("2024-05-21T08:31:28.749Z", data_2["end_timestamp"])
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
