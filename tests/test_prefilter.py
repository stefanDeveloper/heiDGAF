import json
import unittest
from unittest.mock import MagicMock, patch

from src.prefilter.prefilter import Prefilter


class TestInit(unittest.TestCase):
    @patch("src.prefilter.prefilter.KafkaConsumeHandler")
    @patch("src.prefilter.prefilter.KafkaProduceHandler")
    def test_valid_init_no_entry(self, mock_produce_handler, mock_consume_handler):
        sut = Prefilter(error_type=[])

        self.assertIsNone(sut.begin_timestamp)
        self.assertIsNone(sut.end_timestamp)
        self.assertEqual([], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)
        self.assertEqual([], sut.error_type)
        self.assertEqual(None, sut.subnet_id)

        self.assertIsNotNone(sut.kafka_produce_handler)
        self.assertIsNotNone(sut.kafka_consume_handler)

        mock_produce_handler.assert_called_once_with(transactional_id="prefilter")
        mock_consume_handler.assert_called_once_with(topic="Prefilter")

    @patch("src.prefilter.prefilter.KafkaConsumeHandler")
    @patch("src.prefilter.prefilter.KafkaProduceHandler")
    def test_valid_init_one_entry(self, mock_produce_handler, mock_consume_handler):
        sut = Prefilter(error_type=["NXDOMAIN"])

        self.assertIsNone(sut.begin_timestamp)
        self.assertIsNone(sut.end_timestamp)
        self.assertEqual([], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)
        self.assertEqual(["NXDOMAIN"], sut.error_type)
        self.assertEqual(None, sut.subnet_id)

        self.assertIsNotNone(sut.kafka_produce_handler)
        self.assertIsNotNone(sut.kafka_consume_handler)

        mock_produce_handler.assert_called_once_with(transactional_id="prefilter")
        mock_consume_handler.assert_called_once_with(topic="Prefilter")

    @patch("src.prefilter.prefilter.KafkaConsumeHandler")
    @patch("src.prefilter.prefilter.KafkaProduceHandler")
    def test_valid_init_multiple_entries(self, mock_produce_handler, mock_consume_handler):
        sut = Prefilter(error_type=["NXDOMAIN", "OTHER_TYPE"])

        self.assertIsNone(sut.begin_timestamp)
        self.assertIsNone(sut.end_timestamp)
        self.assertEqual([], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)
        self.assertEqual(["NXDOMAIN", "OTHER_TYPE"], sut.error_type)
        self.assertEqual(None, sut.subnet_id)

        self.assertIsNotNone(sut.kafka_produce_handler)
        self.assertIsNotNone(sut.kafka_consume_handler)

        mock_produce_handler.assert_called_once_with(transactional_id="prefilter")
        mock_consume_handler.assert_called_once_with(topic="Prefilter")


class TestGetAndFillData(unittest.TestCase):
    @patch("src.prefilter.prefilter.KafkaConsumeHandler")
    @patch("src.prefilter.prefilter.KafkaProduceHandler")
    def test_get_data_without_new_data(self, mock_produce_handler, mock_consume_handler):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance
        mock_consume_handler_instance = MagicMock()
        mock_consume_handler.return_value = mock_consume_handler_instance
        mock_consume_handler_instance.consume_and_return_json_data.return_value = None, {}

        sut = Prefilter(error_type=["NXDOMAIN"])
        sut.get_and_fill_data()

        self.assertEqual([], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)
        self.assertEqual(None, sut.subnet_id)

        mock_consume_handler_instance.consume_and_return_json_data.assert_called_once()

    @patch("src.prefilter.prefilter.KafkaConsumeHandler")
    @patch("src.prefilter.prefilter.KafkaProduceHandler")
    def test_get_data_with_new_data(self, mock_produce_handler, mock_consume_handler):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance
        mock_consume_handler_instance = MagicMock()
        mock_consume_handler.return_value = mock_consume_handler_instance
        mock_consume_handler_instance.consume_and_return_json_data.return_value = "127.0.0.0/24", {
            "begin_timestamp": "2024-05-21T08:31:28.119Z",
            "end_timestamp": "2024-05-21T08:31:29.432Z",
            "data": ["test_data_1", "test_data_2"],
        }

        sut = Prefilter(error_type=["NXDOMAIN"])
        sut.get_and_fill_data()

        self.assertEqual(["test_data_1", "test_data_2"], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)
        self.assertEqual("127.0.0.0/24", sut.subnet_id)

        mock_consume_handler_instance.consume_and_return_json_data.assert_called_once()

    @patch("src.prefilter.prefilter.KafkaConsumeHandler")
    @patch("src.prefilter.prefilter.KafkaProduceHandler")
    def test_get_data_with_existing_data(self, mock_batch_handler, mock_consume_handler):
        mock_batch_handler_instance = MagicMock()
        mock_batch_handler.return_value = mock_batch_handler_instance
        mock_consume_handler_instance = MagicMock()
        mock_consume_handler.return_value = mock_consume_handler_instance
        mock_consume_handler_instance.consume_and_return_json_data.return_value = "127.0.0.0/24", {
            "begin_timestamp": "2024-05-21T08:31:28.119Z",
            "end_timestamp": "2024-05-21T08:31:29.432Z",
            "data": ["test_data_1", "test_data_2"],
        }

        sut = Prefilter(error_type=["NXDOMAIN"])
        sut.unfiltered_data = ["old_test_data_1", "old_test_data_2"]
        sut.get_and_fill_data()

        self.assertEqual(["test_data_1", "test_data_2"], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)
        self.assertEqual("127.0.0.0/24", sut.subnet_id)

        mock_consume_handler_instance.consume_and_return_json_data.assert_called_once()


class TestFilterByError(unittest.TestCase):
    @patch("src.prefilter.prefilter.KafkaConsumeHandler")
    @patch("src.prefilter.prefilter.KafkaProduceHandler")
    def test_filter_by_error_empty_data(self, mock_produce_handler, mock_consume_handler):
        sut = Prefilter(error_type=["NXDOMAIN"])
        sut.unfiltered_data = []
        sut.filter_by_error()

        self.assertEqual([], sut.filtered_data)

    @patch("src.prefilter.prefilter.KafkaConsumeHandler")
    @patch("src.prefilter.prefilter.KafkaProduceHandler")
    def test_filter_by_error_with_data_no_error_types(self, mock_produce_handler, mock_consume_handler):
        first_entry = json.dumps({
            "timestamp": "2024-05-21T08:31:28.119Z",
            "status": "NOERROR",
            "client_ip": "192.168.0.105",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.heidelberg-botanik.de",
            "record_type": "A",
            "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
            "size": "150b",
        })
        second_entry = json.dumps({
            "timestamp": "2024-06-01T02:31:07.943Z",
            "status": "NXDOMAIN",
            "client_ip": "192.168.1.206",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.biotech-hei.com",
            "record_type": "AAAA",
            "response_ip": "4250:5939:b4f2:b3ec:36ef:752d:b325:189b",
            "size": "117b",
        })
        third_entry = json.dumps({
            "timestamp": "2024-06-01T01:37:41.796Z",
            "status": "NXDOMAIN",
            "client_ip": "192.168.1.206",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.heidelberg-stadtbibliothek.de",
            "record_type": "A",
            "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
            "size": "150b",
        })

        sut = Prefilter(error_type=[])
        sut.unfiltered_data = [first_entry, second_entry, third_entry]
        sut.filter_by_error()

        self.assertEqual([], sut.filtered_data)

    @patch("src.prefilter.prefilter.KafkaConsumeHandler")
    @patch("src.prefilter.prefilter.KafkaProduceHandler")
    def test_filter_by_error_with_data_one_error_type(self, mock_produce_handler, mock_consume_handler):
        first_entry = json.dumps({
            "timestamp": "2024-05-21T08:31:28.119Z",
            "status": "NOERROR",
            "client_ip": "192.168.0.105",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.heidelberg-botanik.de",
            "record_type": "A",
            "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
            "size": "150b",
        })
        second_entry = json.dumps({
            "timestamp": "2024-06-01T02:31:07.943Z",
            "status": "NXDOMAIN",
            "client_ip": "192.168.1.206",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.biotech-hei.com",
            "record_type": "AAAA",
            "response_ip": "4250:5939:b4f2:b3ec:36ef:752d:b325:189b",
            "size": "117b",
        })
        third_entry = json.dumps({
            "timestamp": "2024-06-01T01:37:41.796Z",
            "status": "NXDOMAIN",
            "client_ip": "192.168.1.206",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.heidelberg-stadtbibliothek.de",
            "record_type": "A",
            "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
            "size": "150b",
        })

        sut = Prefilter(error_type=["NXDOMAIN"])
        sut.unfiltered_data = [first_entry, second_entry, third_entry]
        sut.filter_by_error()

        self.assertEqual([second_entry, third_entry], sut.filtered_data)

    @patch("src.prefilter.prefilter.KafkaConsumeHandler")
    @patch("src.prefilter.prefilter.KafkaProduceHandler")
    def test_filter_by_error_with_data_two_error_types(self, mock_produce_handler, mock_consume_handler):
        first_entry = json.dumps({
            "timestamp": "2024-05-21T08:31:28.119Z",
            "status": "NOERROR",
            "client_ip": "192.168.0.105",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.heidelberg-botanik.de",
            "record_type": "A",
            "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
            "size": "150b",
        })
        second_entry = json.dumps({
            "timestamp": "2024-06-01T02:31:07.943Z",
            "status": "NXDOMAIN",
            "client_ip": "192.168.1.206",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.biotech-hei.com",
            "record_type": "AAAA",
            "response_ip": "4250:5939:b4f2:b3ec:36ef:752d:b325:189b",
            "size": "117b",
        })
        third_entry = json.dumps({
            "timestamp": "2024-06-01T01:37:41.796Z",
            "status": "OTHER_TYPE",
            "client_ip": "192.168.1.206",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.heidelberg-stadtbibliothek.de",
            "record_type": "A",
            "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
            "size": "150b",
        })

        sut = Prefilter(error_type=["NXDOMAIN", "OTHER_TYPE"])
        sut.unfiltered_data = [first_entry, second_entry, third_entry]
        sut.filter_by_error()

        self.assertEqual([second_entry, third_entry], sut.filtered_data)


class TestSendFilteredData(unittest.TestCase):
    @patch("src.prefilter.prefilter.KafkaConsumeHandler")
    @patch("src.prefilter.prefilter.KafkaProduceHandler")
    def test_send_with_data(self, mock_produce_handler, mock_consume_handler):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        first_entry = {
            "timestamp": "2024-05-21T08:31:28.119Z",
            "status": "NXDOMAIN",
            "client_ip": "192.168.1.105",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.heidelberg-botanik.de",
            "record_type": "A",
            "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
            "size": "150b",
        }
        second_entry = {
            "timestamp": "2024-06-01T02:31:07.943Z",
            "status": "NXDOMAIN",
            "client_ip": "192.168.1.206",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.biotech-hei.com",
            "record_type": "AAAA",
            "response_ip": "4250:5939:b4f2:b3ec:36ef:752d:b325:189b",
            "size": "117b",
        }

        sut = Prefilter(error_type=["NXDOMAIN"])
        sut.unfiltered_data = [first_entry, second_entry]
        sut.filtered_data = [first_entry, second_entry]
        sut.subnet_id = "192.168.1.0_24"
        sut.begin_timestamp = "2024-05-21T08:31:27.000Z"
        sut.end_timestamp = "2024-05-21T08:31:29.000Z"
        expected_message = (
            '{"begin_timestamp": "2024-05-21T08:31:27.000Z", "end_timestamp": "2024-05-21T08:31:29.000Z", "data": [{'
            '"timestamp": "2024-05-21T08:31:28.119Z", "status": "NXDOMAIN", "client_ip": "192.168.1.105", '
            '"dns_ip": "8.8.8.8", "host_domain_name": "www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}, {"timestamp": "2024-06-01T02:31:07.943Z", '
            '"status": "NXDOMAIN", "client_ip": "192.168.1.206", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.biotech-hei.com", "record_type": "AAAA", "response_ip": "4250:5939:b4f2:b3ec:36ef:752d:b325:189b", '
            '"size": "117b"}]}'
        )
        sut.send_filtered_data()

        mock_produce_handler_instance.send.assert_called_once_with(
            topic="Inspect", data=expected_message, key="192.168.1.0_24",
        )

    @patch("src.prefilter.prefilter.KafkaConsumeHandler")
    @patch("src.prefilter.prefilter.KafkaProduceHandler")
    def test_send_without_filtered_data_with_unfiltered_data(self, mock_produce_handler, mock_consume_handler):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Prefilter(error_type=["NXDOMAIN"])
        sut.unfiltered_data = ["message"]
        sut.filtered_data = []

        with self.assertRaises(ValueError):
            sut.send_filtered_data()

        mock_produce_handler.add_message.assert_not_called()

    @patch("src.prefilter.prefilter.KafkaConsumeHandler")
    @patch("src.prefilter.prefilter.KafkaProduceHandler")
    def test_send_without_data(self, mock_produce_handler, mock_consume_handler):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Prefilter(error_type=["NXDOMAIN"])
        sut.unfiltered_data = []
        sut.filtered_data = []

        self.assertIsNone(sut.send_filtered_data())

        mock_produce_handler.add_message.assert_not_called()


class TestClearData(unittest.TestCase):
    @patch("src.prefilter.prefilter.KafkaConsumeHandler")
    @patch("src.prefilter.prefilter.KafkaProduceHandler")
    def test_clear_data_with_data(self, mock_produce_handler, mock_consume_handler):
        first_entry = {
            "timestamp": "2024-05-21T08:31:28.119Z",
            "status": "NOERROR",
            "client_ip": "192.168.0.105",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.heidelberg-botanik.de",
            "record_type": "A",
            "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
            "size": "150b",
        }
        second_entry = {
            "timestamp": "2024-06-01T02:31:07.943Z",
            "status": "NXDOMAIN",
            "client_ip": "192.168.1.206",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.biotech-hei.com",
            "record_type": "AAAA",
            "response_ip": "4250:5939:b4f2:b3ec:36ef:752d:b325:189b",
            "size": "117b",
        }

        sut = Prefilter(error_type=["NXDOMAIN"])
        sut.unfiltered_data = [first_entry, second_entry]
        sut.filtered_data = [second_entry]
        sut.clear_data()

        self.assertEqual([], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)

    @patch("src.prefilter.prefilter.KafkaConsumeHandler")
    @patch("src.prefilter.prefilter.KafkaProduceHandler")
    def test_clear_data_without_data(self, mock_produce_handler, mock_consume_handler):
        sut = Prefilter(error_type=["NXDOMAIN"])
        sut.unfiltered_data = []
        sut.filtered_data = []
        sut.clear_data()

        self.assertEqual([], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)


if __name__ == "__main__":
    unittest.main()
