import unittest
from unittest.mock import MagicMock, patch

from heidgaf_log_collection.prefilter import Prefilter


class TestInit(unittest.TestCase):
    @patch("heidgaf_log_collection.prefilter.KafkaConsumeHandler")
    @patch("heidgaf_log_collection.prefilter.KafkaBatchSender")
    def test_valid_init(self, mock_batch_handler, mock_consume_handler):
        sut = Prefilter(error_type="NXDOMAIN")

        self.assertEqual([], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)
        self.assertEqual("NXDOMAIN", sut.error_type)

        self.assertIsNotNone(sut.batch_handler)

        mock_batch_handler.assert_called_once_with(
            topic="Inspect", transactional_id="prefilter"
        )
        mock_consume_handler.assert_called_once_with(topic="Prefilter")


class TestGetAndFillData(unittest.TestCase):
    @patch("heidgaf_log_collection.prefilter.KafkaConsumeHandler")
    @patch("heidgaf_log_collection.prefilter.KafkaBatchSender")
    def test_get_data_without_new_data(self, mock_batch_handler, mock_consume_handler):
        mock_batch_handler_instance = MagicMock()
        mock_batch_handler.return_value = mock_batch_handler_instance
        mock_consume_handler_instance = MagicMock()
        mock_consume_handler.return_value = mock_consume_handler_instance
        mock_consume_handler_instance.consume_and_return_json_data.return_value = []

        sut = Prefilter(error_type="NXDOMAIN")
        sut.get_and_fill_data()

        self.assertEqual([], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)

        mock_consume_handler_instance.consume_and_return_json_data.assert_called_once()

    @patch("heidgaf_log_collection.prefilter.KafkaConsumeHandler")
    @patch("heidgaf_log_collection.prefilter.KafkaBatchSender")
    def test_get_data_with_new_data(self, mock_batch_handler, mock_consume_handler):
        mock_batch_handler_instance = MagicMock()
        mock_batch_handler.return_value = mock_batch_handler_instance
        mock_consume_handler_instance = MagicMock()
        mock_consume_handler.return_value = mock_consume_handler_instance
        mock_consume_handler_instance.consume_and_return_json_data.return_value = [
            "test_data"
        ]

        sut = Prefilter(error_type="NXDOMAIN")
        sut.get_and_fill_data()

        self.assertEqual(["test_data"], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)

        mock_consume_handler_instance.consume_and_return_json_data.assert_called_once()

    @patch("heidgaf_log_collection.prefilter.KafkaConsumeHandler")
    @patch("heidgaf_log_collection.prefilter.KafkaBatchSender")
    def test_get_data_with_existing_data(
        self, mock_batch_handler, mock_consume_handler
    ):
        sut = Prefilter(error_type="NXDOMAIN")


class TestFilterByError(unittest.TestCase):
    @patch("heidgaf_log_collection.prefilter.KafkaConsumeHandler")
    @patch("heidgaf_log_collection.prefilter.KafkaBatchSender")
    def test_filter_by_error_empty_data(self, mock_batch_handler, mock_consume_handler):
        sut = Prefilter(error_type="NXDOMAIN")
        sut.unfiltered_data = []
        sut.filter_by_error()

        self.assertEqual([], sut.filtered_data)

    @patch("heidgaf_log_collection.prefilter.KafkaConsumeHandler")
    @patch("heidgaf_log_collection.prefilter.KafkaBatchSender")
    def test_filter_by_error_with_data(self, mock_batch_handler, mock_consume_handler):
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
        third_entry = {
            "timestamp": "2024-06-01T01:37:41.796Z",
            "status": "NXDOMAIN",
            "client_ip": "192.168.1.206",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.heidelberg-stadtbibliothek.de",
            "record_type": "A",
            "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
            "size": "150b",
        }

        sut = Prefilter(error_type="NXDOMAIN")
        sut.unfiltered_data = [first_entry, second_entry, third_entry]
        sut.filter_by_error()

        self.assertEqual([second_entry, third_entry], sut.filtered_data)


class TestAddFilteredDataToBatch(unittest.TestCase):
    @patch("heidgaf_log_collection.prefilter.KafkaConsumeHandler")
    @patch("heidgaf_log_collection.prefilter.KafkaBatchSender")
    def test_add_to_batch_with_data(self, mock_batch_handler, mock_consume_handler):
        mock_batch_handler_instance = MagicMock()
        mock_batch_handler.return_value = mock_batch_handler_instance

        first_entry = {
            "timestamp": "2024-05-21T08:31:28.119Z",
            "status": "NXDOMAIN",
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

        sut = Prefilter(error_type="NXDOMAIN")
        sut.filtered_data = [first_entry, second_entry]
        expected_message = (
            '[{"timestamp": "2024-05-21T08:31:28.119Z", "status": "NXDOMAIN", "client_ip": '
            '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": "www.heidelberg-botanik.de", '
            '"record_type": "A", "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", '
            '"size": "150b"}, {"timestamp": "2024-06-01T02:31:07.943Z", "status": "NXDOMAIN", '
            '"client_ip": "192.168.1.206", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.biotech-hei.com", "record_type": "AAAA", "response_ip": '
            '"4250:5939:b4f2:b3ec:36ef:752d:b325:189b", "size": "117b"}]'
        )
        sut.add_filtered_data_to_batch()

        mock_batch_handler_instance.add_message.assert_called_once_with(
            expected_message
        )

    @patch("heidgaf_log_collection.prefilter.KafkaConsumeHandler")
    @patch("heidgaf_log_collection.prefilter.KafkaBatchSender")
    def test_add_to_batch_without_data(self, mock_batch_handler, mock_consume_handler):
        mock_batch_handler_instance = MagicMock()
        mock_batch_handler.return_value = mock_batch_handler_instance

        sut = Prefilter(error_type="NXDOMAIN")
        sut.filtered_data = []

        with self.assertRaises(ValueError):
            sut.add_filtered_data_to_batch()

        mock_batch_handler.add_message.assert_not_called()


class TestClearData(unittest.TestCase):
    @patch("heidgaf_log_collection.prefilter.KafkaConsumeHandler")
    @patch("heidgaf_log_collection.prefilter.KafkaBatchSender")
    def test_clear_data_with_data(self, mock_batch_handler, mock_consume_handler):
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

        sut = Prefilter(error_type="NXDOMAIN")
        sut.unfiltered_data = [first_entry, second_entry]
        sut.filtered_data = [second_entry]
        sut.clear_data()

        self.assertEqual([], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)

    @patch("heidgaf_log_collection.prefilter.KafkaConsumeHandler")
    @patch("heidgaf_log_collection.prefilter.KafkaBatchSender")
    def test_clear_data_without_data(self, mock_batch_handler, mock_consume_handler):
        sut = Prefilter(error_type="NXDOMAIN")
        sut.unfiltered_data = []
        sut.filtered_data = []
        sut.clear_data()

        self.assertEqual([], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)


if __name__ == "__main__":
    unittest.main()
