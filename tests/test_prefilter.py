import json
import unittest
from unittest.mock import MagicMock, patch

from src.base.kafka_handler import KafkaMessageFetchException
from src.prefilter.prefilter import Prefilter, main


class TestInit(unittest.TestCase):
    @patch("src.prefilter.prefilter.CONSUME_TOPIC", "test_topic")
    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    def test_valid_init(
        self, mock_produce_handler, mock_consume_handler, mock_logline_handler
    ):
        sut = Prefilter()

        self.assertIsNone(sut.begin_timestamp)
        self.assertIsNone(sut.end_timestamp)
        self.assertEqual([], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)
        self.assertEqual(None, sut.subnet_id)

        self.assertIsNotNone(sut.kafka_produce_handler)
        self.assertIsNotNone(sut.kafka_consume_handler)
        self.assertIsNotNone(sut.logline_handler)

        mock_produce_handler.assert_called_once()
        mock_consume_handler.assert_called_once_with("test_topic")
        mock_logline_handler.assert_called_once()


class TestGetAndFillData(unittest.TestCase):
    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    def test_get_data_without_new_data(
        self,
        mock_produce_handler,
        mock_consume_handler,
        mock_logline_handler,
        mock_logger,
    ):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance
        mock_consume_handler_instance = MagicMock()
        mock_consume_handler.return_value = mock_consume_handler_instance
        mock_consume_handler_instance.consume_as_json.return_value = (
            None,
            {},
        )

        sut = Prefilter()
        sut.get_and_fill_data()

        self.assertEqual([], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)
        self.assertEqual(None, sut.subnet_id)

        mock_consume_handler_instance.consume_as_json.assert_called_once()

    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    def test_get_data_with_new_data(
        self,
        mock_produce_handler,
        mock_consume_handler,
        mock_logline_handler,
        mock_logger,
    ):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance
        mock_consume_handler_instance = MagicMock()
        mock_consume_handler.return_value = mock_consume_handler_instance
        mock_consume_handler_instance.consume_as_json.return_value = (
            "127.0.0.0/24",
            {
                "begin_timestamp": "2024-05-21T08:31:28.119Z",
                "end_timestamp": "2024-05-21T08:31:29.432Z",
                "data": ["test_data_1", "test_data_2"],
            },
        )

        sut = Prefilter()
        sut.get_and_fill_data()

        self.assertEqual(["test_data_1", "test_data_2"], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)
        self.assertEqual("127.0.0.0/24", sut.subnet_id)

        mock_consume_handler_instance.consume_as_json.assert_called_once()

    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    def test_get_data_with_existing_data(
        self,
        mock_batch_handler,
        mock_consume_handler,
        mock_logline_handler,
        mock_logger,
    ):
        mock_batch_handler_instance = MagicMock()
        mock_batch_handler.return_value = mock_batch_handler_instance
        mock_consume_handler_instance = MagicMock()
        mock_consume_handler.return_value = mock_consume_handler_instance
        mock_consume_handler_instance.consume_as_json.return_value = (
            "127.0.0.0/24",
            {
                "begin_timestamp": "2024-05-21T08:31:28.119Z",
                "end_timestamp": "2024-05-21T08:31:29.432Z",
                "data": ["test_data_1", "test_data_2"],
            },
        )

        sut = Prefilter()
        sut.unfiltered_data = ["old_test_data_1", "old_test_data_2"]
        sut.get_and_fill_data()

        self.assertEqual(["test_data_1", "test_data_2"], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)
        self.assertEqual("127.0.0.0/24", sut.subnet_id)

        mock_consume_handler_instance.consume_as_json.assert_called_once()


class TestFilterByError(unittest.TestCase):
    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    def test_filter_by_error_empty_data(
        self,
        mock_produce_handler,
        mock_consume_handler,
        mock_logline_handler,
        mock_logger,
    ):
        sut = Prefilter()
        sut.unfiltered_data = []

        sut.filter_by_error()

        self.assertEqual([], sut.filtered_data)

    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    def test_filter_by_error_with_data_no_error_types(
        self,
        mock_produce_handler,
        mock_consume_handler,
        mock_logline_handler,
        mock_logger,
    ):
        first_entry = json.dumps(
            {
                "timestamp": "2024-05-21T08:31:28.119Z",
                "status_code": "NOERROR",
                "client_ip": "192.168.0.105",
                "dns_ip": "8.8.8.8",
                "host_domain_name": "www.heidelberg-botanik.de",
                "record_type": "A",
                "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
                "size": "150b",
            }
        )
        second_entry = json.dumps(
            {
                "timestamp": "2024-06-01T02:31:07.943Z",
                "status_code": "NXDOMAIN",
                "client_ip": "192.168.1.206",
                "dns_ip": "8.8.8.8",
                "host_domain_name": "www.biotech-hei.com",
                "record_type": "AAAA",
                "response_ip": "4250:5939:b4f2:b3ec:36ef:752d:b325:189b",
                "size": "117b",
            }
        )
        third_entry = json.dumps(
            {
                "timestamp": "2024-06-01T01:37:41.796Z",
                "status_code": "NXDOMAIN",
                "client_ip": "192.168.1.206",
                "dns_ip": "8.8.8.8",
                "host_domain_name": "www.heidelberg-stadtbibliothek.de",
                "record_type": "A",
                "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
                "size": "150b",
            }
        )

        sut = Prefilter()
        sut.unfiltered_data = [first_entry, second_entry, third_entry]
        sut.logline_handler.check_relevance.side_effect = [False, False, False]

        sut.filter_by_error()

        self.assertEqual([], sut.filtered_data)

    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    def test_filter_by_error_with_data_one_error_type(
        self,
        mock_produce_handler,
        mock_consume_handler,
        mock_logline_handler,
        mock_logger,
    ):
        first_entry = json.dumps(
            {
                "timestamp": "2024-05-21T08:31:28.119Z",
                "status_code": "NOERROR",
                "client_ip": "192.168.0.105",
                "dns_ip": "8.8.8.8",
                "host_domain_name": "www.heidelberg-botanik.de",
                "record_type": "A",
                "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
                "size": "150b",
            }
        )
        second_entry = json.dumps(
            {
                "timestamp": "2024-06-01T02:31:07.943Z",
                "status_code": "NXDOMAIN",
                "client_ip": "192.168.1.206",
                "dns_ip": "8.8.8.8",
                "host_domain_name": "www.biotech-hei.com",
                "record_type": "AAAA",
                "response_ip": "4250:5939:b4f2:b3ec:36ef:752d:b325:189b",
                "size": "117b",
            }
        )
        third_entry = json.dumps(
            {
                "timestamp": "2024-06-01T01:37:41.796Z",
                "status_code": "NXDOMAIN",
                "client_ip": "192.168.1.206",
                "dns_ip": "8.8.8.8",
                "host_domain_name": "www.heidelberg-stadtbibliothek.de",
                "record_type": "A",
                "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
                "size": "150b",
            }
        )

        sut = Prefilter()
        sut.unfiltered_data = [first_entry, second_entry, third_entry]
        sut.logline_handler.check_relevance.side_effect = [False, True, True]

        sut.filter_by_error()

        self.assertEqual([second_entry, third_entry], sut.filtered_data)

    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    def test_filter_by_error_with_data_two_error_types(
        self,
        mock_produce_handler,
        mock_consume_handler,
        mock_logline_handler,
        mock_logger,
    ):
        first_entry = json.dumps(
            {
                "timestamp": "2024-05-21T08:31:28.119Z",
                "status_code": "NOERROR",
                "client_ip": "192.168.0.105",
                "dns_ip": "8.8.8.8",
                "host_domain_name": "www.heidelberg-botanik.de",
                "record_type": "A",
                "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
                "size": "150b",
            }
        )
        second_entry = json.dumps(
            {
                "timestamp": "2024-06-01T02:31:07.943Z",
                "status_code": "NXDOMAIN",
                "client_ip": "192.168.1.206",
                "dns_ip": "8.8.8.8",
                "host_domain_name": "www.biotech-hei.com",
                "record_type": "AAAA",
                "response_ip": "4250:5939:b4f2:b3ec:36ef:752d:b325:189b",
                "size": "117b",
            }
        )
        third_entry = json.dumps(
            {
                "timestamp": "2024-06-01T01:37:41.796Z",
                "status_code": "OTHER_TYPE",
                "client_ip": "192.168.1.206",
                "dns_ip": "8.8.8.8",
                "host_domain_name": "www.heidelberg-stadtbibliothek.de",
                "record_type": "A",
                "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
                "size": "150b",
            }
        )

        sut = Prefilter()
        sut.unfiltered_data = [first_entry, second_entry, third_entry]
        sut.logline_handler.check_relevance.side_effect = [False, True, True]

        sut.filter_by_error()

        self.assertEqual([second_entry, third_entry], sut.filtered_data)


class TestSendFilteredData(unittest.TestCase):
    @patch("src.prefilter.prefilter.PRODUCE_TOPIC", "test_topic")
    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    def test_send_with_data(
        self,
        mock_produce_handler,
        mock_consume_handler,
        mock_logline_handler,
        mock_logger,
    ):
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

        sut = Prefilter()
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

        mock_produce_handler_instance.produce.assert_called_once_with(
            topic="test_topic",
            data=expected_message,
            key="192.168.1.0_24",
        )

    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    def test_send_without_filtered_data_with_unfiltered_data(
        self,
        mock_produce_handler,
        mock_consume_handler,
        mock_logline_handler,
        mock_logger,
    ):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Prefilter()
        sut.unfiltered_data = ["message"]
        sut.filtered_data = []

        with self.assertRaises(ValueError):
            sut.send_filtered_data()

        mock_produce_handler.add_message.assert_not_called()

    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    def test_send_without_data(
        self, mock_produce_handler, mock_consume_handler, mock_logline_handler
    ):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Prefilter()
        sut.unfiltered_data = []
        sut.filtered_data = []

        self.assertIsNone(sut.send_filtered_data())

        mock_produce_handler.add_message.assert_not_called()


class TestClearData(unittest.TestCase):
    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    def test_clear_data_with_data(
        self, mock_produce_handler, mock_consume_handler, mock_logline_handler
    ):
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

        sut = Prefilter()
        sut.unfiltered_data = [first_entry, second_entry]
        sut.filtered_data = [second_entry]
        sut.clear_data()

        self.assertEqual([], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)

    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    def test_clear_data_without_data(
        self, mock_produce_handler, mock_consume_handler, mock_logline_handler
    ):
        sut = Prefilter()
        sut.unfiltered_data = []
        sut.filtered_data = []
        sut.clear_data()

        self.assertEqual([], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)


class TestMainFunction(unittest.IsolatedAsyncioTestCase):
    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.Prefilter")
    async def test_main_normal_flow(self, mock_prefilter, mock_logger):
        # Arrange
        mock_prefilter_instance = mock_prefilter.return_value
        mock_prefilter_instance.get_and_fill_data.return_value = MagicMock()
        mock_prefilter_instance.filter_by_error.return_value = MagicMock()
        mock_prefilter_instance.send_filtered_data.return_value = MagicMock()
        mock_prefilter_instance.clear_data.return_value = MagicMock()

        # Act
        main(one_iteration=True)

        # Assert
        mock_prefilter_instance.get_and_fill_data.assert_called()
        mock_prefilter_instance.filter_by_error.assert_called()
        mock_prefilter_instance.send_filtered_data.assert_called()
        mock_prefilter_instance.clear_data.assert_called()

    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.Prefilter")
    async def test_main_ioerror(self, mock_prefilter, mock_logger):
        # Arrange
        mock_prefilter_instance = mock_prefilter.return_value
        mock_prefilter_instance.clear_data.return_value = MagicMock()
        mock_prefilter_instance.get_and_fill_data.side_effect = IOError

        # Act and Assert
        with self.assertRaises(IOError):
            main(one_iteration=True)

        mock_prefilter_instance.clear_data.assert_called()

    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.Prefilter")
    async def test_main_valueerror(self, mock_prefilter, mock_logger):
        # Arrange
        mock_prefilter_instance = mock_prefilter.return_value
        mock_prefilter_instance.clear_data.return_value = MagicMock()
        mock_prefilter_instance.get_and_fill_data.side_effect = ValueError

        # Act
        main(one_iteration=True)

        # Assert
        mock_prefilter_instance.clear_data.assert_called()

    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.Prefilter")
    async def test_main_kafka_message_fetch_exception(
        self, mock_prefilter, mock_logger
    ):
        # Arrange
        mock_prefilter_instance = mock_prefilter.return_value
        mock_prefilter_instance.clear_data.return_value = MagicMock()
        mock_prefilter_instance.get_and_fill_data.side_effect = (
            KafkaMessageFetchException
        )

        # Act
        main(one_iteration=True)

        # Assert
        mock_prefilter_instance.clear_data.assert_called()

    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.Prefilter")
    async def test_main_keyboard_interrupt(self, mock_prefilter, mock_logger):
        # Arrange
        mock_prefilter_instance = mock_prefilter.return_value
        mock_prefilter_instance.clear_data.return_value = MagicMock()
        mock_prefilter_instance.get_and_fill_data.side_effect = KeyboardInterrupt

        # Act
        main()

        # Assert
        mock_prefilter_instance.clear_data.assert_called()


if __name__ == "__main__":
    unittest.main()
