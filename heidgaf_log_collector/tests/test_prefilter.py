import unittest
from unittest.mock import patch

from heidgaf_log_collector.prefilter import InspectPrefilter


class TestInit(unittest.TestCase):
    @patch('heidgaf_log_collector.prefilter.KafkaBatchSender')
    def test_valid_init(self, mock_batch_handler):
        prefilter_instance = InspectPrefilter(error_type="NXDOMAIN")

        mock_batch_handler.assert_called_once_with(topic="Inspect")
        self.assertEqual([], prefilter_instance.unfiltered_data)
        self.assertEqual([], prefilter_instance.filtered_data)
        self.assertEqual("NXDOMAIN", prefilter_instance.error_type)
        self.assertIsNotNone(prefilter_instance.batch_handler)
        prefilter_instance.batch_handler.start_kafka_producer.assert_called_once()


class TestFilterByError(unittest.TestCase):
    def test_filter_by_error_empty_data(self):
        prefilter_instance = InspectPrefilter(error_type="NXDOMAIN")
        prefilter_instance.unfiltered_data = []

        prefilter_instance.filter_by_error()

        self.assertEqual([], prefilter_instance.filtered_data)

    def test_filter_by_error_with_data(self):
        prefilter_instance = InspectPrefilter(error_type="NXDOMAIN")

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

        prefilter_instance.unfiltered_data = [first_entry, second_entry, third_entry]

        prefilter_instance.filter_by_error()

        self.assertEqual([second_entry, third_entry], prefilter_instance.filtered_data)


class TestAddFilteredDataToBatch(unittest.TestCase):
    @patch('heidgaf_log_collector.prefilter.KafkaBatchSender')
    def test_add_to_batch_with_data(self, mock_batch_handler):
        prefilter_instance = InspectPrefilter(error_type="NXDOMAIN")

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

        prefilter_instance.filtered_data = [first_entry, second_entry]

        prefilter_instance.batch_handler = mock_batch_handler
        expected_message = ('[{"timestamp": "2024-05-21T08:31:28.119Z", "status": "NXDOMAIN", "client_ip": '
                            '"192.168.0.105", "dns_ip": "8.8.8.8", "host_domain_name": "www.heidelberg-botanik.de", '
                            '"record_type": "A", "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", '
                            '"size": "150b"}, {"timestamp": "2024-06-01T02:31:07.943Z", "status": "NXDOMAIN", '
                            '"client_ip": "192.168.1.206", "dns_ip": "8.8.8.8", "host_domain_name": '
                            '"www.biotech-hei.com", "record_type": "AAAA", "response_ip": '
                            '"4250:5939:b4f2:b3ec:36ef:752d:b325:189b", "size": "117b"}]')

        prefilter_instance.add_filtered_data_to_batch()

        mock_batch_handler.add_message.assert_called_once_with(expected_message)

    @patch('heidgaf_log_collector.prefilter.KafkaBatchSender')
    def test_add_to_batch_without_data(self, mock_batch_handler):
        prefilter_instance = InspectPrefilter(error_type="NXDOMAIN")
        prefilter_instance.filtered_data = []
        prefilter_instance.batch_handler = mock_batch_handler

        with self.assertRaises(ValueError):
            prefilter_instance.add_filtered_data_to_batch()

        mock_batch_handler.add_message.assert_not_called()


class TestClearData(unittest.TestCase):
    def test_clear_data_with_data(self):
        prefilter_instance = InspectPrefilter(error_type="NXDOMAIN")

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

        prefilter_instance.unfiltered_data = [first_entry, second_entry]
        prefilter_instance.filtered_data = [second_entry]

        prefilter_instance.clear_data()

        self.assertEqual([], prefilter_instance.unfiltered_data)
        self.assertEqual([], prefilter_instance.filtered_data)

    def test_clear_data_without_data(self):
        prefilter_instance = InspectPrefilter(error_type="NXDOMAIN")

        prefilter_instance.unfiltered_data = []
        prefilter_instance.filtered_data = []

        prefilter_instance.clear_data()

        self.assertEqual([], prefilter_instance.unfiltered_data)
        self.assertEqual([], prefilter_instance.filtered_data)


if __name__ == '__main__':
    unittest.main()
