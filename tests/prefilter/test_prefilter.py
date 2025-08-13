import datetime
import unittest
import uuid
from unittest.mock import MagicMock, patch, AsyncMock

from src.base.data_classes.batch import Batch
from src.base.kafka_handler import KafkaMessageFetchException
from src.prefilter.prefilter import Prefilter, main


class TestInit(unittest.TestCase):
    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    @patch("src.prefilter.prefilter.ClickHouseKafkaSender")
    def test_valid_init(
        self,
        mock_clickhouse,
        mock_produce_handler,
        mock_consume_handler,
        mock_logline_handler,
    ):
        sut = Prefilter(
            consume_topic="test_topic",
            produce_topics=["produce_topic"],
            relevance_function_name="no_relevance",
            validation_config={}
        )
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
    @patch("src.prefilter.prefilter.ClickHouseKafkaSender")
    def test_get_data_without_new_data(
        self,
        mock_clickhouse,
        mock_produce_handler,
        mock_consume_handler,
        mock_logline_handler,
        mock_logger,
    ):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance
        mock_consume_handler_instance = MagicMock()
        mock_consume_handler.return_value = mock_consume_handler_instance
        mock_consume_handler_instance.consume_as_object.return_value = (
            None,
            Batch(
                batch_tree_row_id=f"{uuid.uuid4()}-{uuid.uuid4()}",
                batch_id=uuid.uuid4(),
                begin_timestamp=datetime.datetime.now(),
                end_timestamp=datetime.datetime.now(),
                data=[],
            ),
        )

        sut = Prefilter(
            consume_topic="test_topic",
            produce_topics=["produce_topic"],
            relevance_function_name="no_relevance",
            validation_config={}
        )
        
        sut.get_and_fill_data()

        self.assertEqual([], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)
        self.assertEqual(None, sut.subnet_id)

        mock_consume_handler_instance.consume_as_object.assert_called_once()

    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    @patch("src.prefilter.prefilter.ClickHouseKafkaSender")
    def test_get_data_with_new_data(
        self,
        mock_clickhouse,
        mock_produce_handler,
        mock_consume_handler,
        mock_logline_handler,
        mock_logger,
    ):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance
        mock_consume_handler_instance = MagicMock()
        mock_consume_handler.return_value = mock_consume_handler_instance
        mock_consume_handler_instance.consume_as_object.return_value = (
            "127.0.0.0_24",
            Batch(
                batch_tree_row_id=f"{uuid.uuid4()}-{uuid.uuid4()}",
                batch_id=uuid.uuid4(),
                begin_timestamp=datetime.datetime.now(),
                end_timestamp=datetime.datetime.now(),
                data=["test_data_1", "test_data_2"],
            ),
        )

        sut = Prefilter(
            consume_topic="test_topic",
            produce_topics=["produce_topic"],
            relevance_function_name="no_relevance",
            validation_config={}
        )
        sut.get_and_fill_data()

        self.assertEqual(["test_data_1", "test_data_2"], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)
        self.assertEqual("127.0.0.0_24", sut.subnet_id)

        mock_consume_handler_instance.consume_as_object.assert_called_once()

    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    @patch("src.prefilter.prefilter.ClickHouseKafkaSender")
    def test_get_data_with_existing_data(
        self,
        mock_clickhouse,
        mock_batch_handler,
        mock_consume_handler,
        mock_logline_handler,
        mock_logger,
    ):
        mock_batch_handler_instance = MagicMock()
        mock_batch_handler.return_value = mock_batch_handler_instance
        mock_consume_handler_instance = MagicMock()
        mock_consume_handler.return_value = mock_consume_handler_instance
        mock_consume_handler_instance.consume_as_object.return_value = (
            "127.0.0.0_24",
            Batch(
                batch_tree_row_id=f"{uuid.uuid4()}-{uuid.uuid4()}",
                batch_id=uuid.uuid4(),
                begin_timestamp=datetime.datetime.now(),
                end_timestamp=datetime.datetime.now(),
                data=["test_data_1", "test_data_2"],
            ),
        )

        sut = Prefilter(
            consume_topic="test_topic",
            produce_topics=["produce_topic"],
            relevance_function_name="no_relevance",
            validation_config={}
        )
        sut.unfiltered_data = ["old_test_data_1", "old_test_data_2"]
        sut.get_and_fill_data()

        self.assertEqual(["test_data_1", "test_data_2"], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)
        self.assertEqual("127.0.0.0_24", sut.subnet_id)

        mock_consume_handler_instance.consume_as_object.assert_called_once()


class TestFilterByError(unittest.TestCase):
    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    @patch("src.prefilter.prefilter.ClickHouseKafkaSender")
    def test_check_data_relevance_using_rules_empty_data(
        self,
        mock_clickhouse,
        mock_produce_handler,
        mock_consume_handler,
        mock_logline_handler,
        mock_logger,
    ):
        sut = Prefilter(
            consume_topic="test_topic",
            produce_topics=["produce_topic"],
            relevance_function_name="no_relevance",
            validation_config={}
        )
        sut.unfiltered_data = []

        sut.check_data_relevance_using_rules()

        self.assertEqual([], sut.filtered_data)

    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    @patch("src.prefilter.prefilter.ClickHouseKafkaSender")
    def test_check_data_relevance_using_rules_with_data_no_error_types(
        self,
        mock_clickhouse,
        mock_produce_handler,
        mock_consume_handler,
        mock_logline_handler,
        mock_logger,
    ):
        first_entry = {
            "logline_id": str(uuid.uuid4()),
            "ts": "2024-05-21T08:31:28.119Z",
            "status_code": "NOERROR",
            "src_ip": "192.168.0.105",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.heidelberg-botanik.de",
            "record_type": "A",
            "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
            "size": "150b",
        }

        second_entry = {
            "logline_id": str(uuid.uuid4()),
            "ts": "2024-06-01T02:31:07.943Z",
            "status_code": "NXDOMAIN",
            "src_ip": "192.168.1.206",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.biotech-hei.com",
            "record_type": "AAAA",
            "response_ip": "4250:5939:b4f2:b3ec:36ef:752d:b325:189b",
            "size": "117b",
        }

        third_entry = {
            "logline_id": str(uuid.uuid4()),
            "ts": "2024-06-01T01:37:41.796Z",
            "status_code": "NXDOMAIN",
            "src_ip": "192.168.1.206",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.heidelberg-stadtbibliothek.de",
            "record_type": "A",
            "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
            "size": "150b",
        }

        sut = Prefilter(
            consume_topic="test_topic",
            produce_topics=["produce_topic"],
            relevance_function_name="no_relevance",
            validation_config={}
        )
        
        sut.unfiltered_data = [first_entry, second_entry, third_entry]
        sut.logline_handler.check_relevance.side_effect = [False, False, False]

        sut.check_data_relevance_using_rules()

        self.assertEqual([], sut.filtered_data)

    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    @patch("src.prefilter.prefilter.ClickHouseKafkaSender")
    def test_check_data_relevance_using_rules_with_data_one_error_type(
        self,
        mock_clickhouse,
        mock_produce_handler,
        mock_consume_handler,
        mock_logline_handler,
        mock_logger,
    ):
        first_entry = {
            "logline_id": str(uuid.uuid4()),
            "ts": "2024-05-21T08:31:28.119Z",
            "status_code": "NOERROR",
            "src_ip": "192.168.0.105",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.heidelberg-botanik.de",
            "record_type": "A",
            "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
            "size": "150b",
        }

        second_entry = {
            "logline_id": str(uuid.uuid4()),
            "ts": "2024-06-01T02:31:07.943Z",
            "status_code": "NXDOMAIN",
            "src_ip": "192.168.1.206",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.biotech-hei.com",
            "record_type": "AAAA",
            "response_ip": "4250:5939:b4f2:b3ec:36ef:752d:b325:189b",
            "size": "117b",
        }

        third_entry = {
            "logline_id": str(uuid.uuid4()),
            "ts": "2024-06-01T01:37:41.796Z",
            "status_code": "NXDOMAIN",
            "src_ip": "192.168.1.206",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.heidelberg-stadtbibliothek.de",
            "record_type": "A",
            "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
            "size": "150b",
        }

        sut = Prefilter(
            consume_topic="test_topic",
            produce_topics=["produce_topic"],
            relevance_function_name="no_relevance",
            validation_config={}
        )
        sut.unfiltered_data = [first_entry, second_entry, third_entry]
        sut.logline_handler.check_relevance.side_effect = [False, True, True]

        sut.check_data_relevance_using_rules()

        self.assertEqual([second_entry, third_entry], sut.filtered_data)

    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    @patch("src.prefilter.prefilter.ClickHouseKafkaSender")
    def test_check_data_relevance_using_rules_with_data_two_error_types(
        self,
        mock_clickhouse,
        mock_produce_handler,
        mock_consume_handler,
        mock_logline_handler,
        mock_logger,
    ):
        first_entry = {
            "logline_id": str(uuid.uuid4()),
            "ts": "2024-05-21T08:31:28.119Z",
            "status_code": "NOERROR",
            "src_ip": "192.168.0.105",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.heidelberg-botanik.de",
            "record_type": "A",
            "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
            "size": "150b",
        }

        second_entry = {
            "logline_id": str(uuid.uuid4()),
            "ts": "2024-06-01T02:31:07.943Z",
            "status_code": "NXDOMAIN",
            "src_ip": "192.168.1.206",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.biotech-hei.com",
            "record_type": "AAAA",
            "response_ip": "4250:5939:b4f2:b3ec:36ef:752d:b325:189b",
            "size": "117b",
        }

        third_entry = {
            "logline_id": str(uuid.uuid4()),
            "ts": "2024-06-01T01:37:41.796Z",
            "status_code": "OTHER_TYPE",
            "src_ip": "192.168.1.206",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.heidelberg-stadtbibliothek.de",
            "record_type": "A",
            "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
            "size": "150b",
        }

        sut = Prefilter(
            consume_topic="test_topic",
            produce_topics=["produce_topic"],
            relevance_function_name="no_relevance",
            validation_config={}
        )
        sut.unfiltered_data = [first_entry, second_entry, third_entry]
        sut.logline_handler.check_relevance.side_effect = [False, True, True]

        sut.check_data_relevance_using_rules()

        self.assertEqual([second_entry, third_entry], sut.filtered_data)


class TestSendFilteredData(unittest.TestCase):
    @patch("src.prefilter.prefilter.generate_collisions_resistant_uuid")
    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    @patch("src.prefilter.prefilter.ClickHouseKafkaSender")
    def test_send_with_data(
        self,
        mock_clickhouse,
        mock_produce_handler,
        mock_consume_handler,
        mock_logline_handler,
        mock_logger,
        mock_generate_uuid
    ):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        first_entry = {
            "ts": "2024-05-21T08:31:28.119Z",
            "status": "NXDOMAIN",
            "src_ip": "192.168.1.105",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.heidelberg-botanik.de",
            "record_type": "A",
            "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
            "size": "150b",
        }
        second_entry = {
            "ts": "2024-06-01T02:31:07.943Z",
            "status": "NXDOMAIN",
            "src_ip": "192.168.1.206",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.biotech-hei.com",
            "record_type": "AAAA",
            "response_ip": "4250:5939:b4f2:b3ec:36ef:752d:b325:189b",
            "size": "117b",
        }

        sut = Prefilter(
            consume_topic="test_topic",
            produce_topics=["produce_topic"],
            relevance_function_name="no_relevance",
            validation_config={}
        )
        mock_generate_uuid.return_value = uuid.UUID('35a21ad1-0bdb-481f-83b0-8c8d4c924f10')
        sut.unfiltered_data = [first_entry, second_entry]
        sut.filtered_data = [first_entry, second_entry]
        sut.subnet_id = "192.168.1.0_24"
        sut.batch_id = uuid.UUID("5236b147-5b0d-44a8-981f-bd7da8c54733")
        sut.parent_row_id = uuid.UUID('35a21ad1-0bdb-481f-83b0-8c8d4c924f10')
        sut.begin_timestamp = datetime.datetime(2024, 5, 21, 8, 31, 27, 000000)
        sut.end_timestamp = datetime.datetime(2024, 5, 21, 8, 31, 29, 000000)
        expected_message = (
            '{"batch_tree_row_id": "35a21ad1-0bdb-481f-83b0-8c8d4c924f10", "batch_id": "5236b147-5b0d-44a8-981f-bd7da8c54733", "begin_timestamp": "2024-05-21T08:31:27", '
            '"end_timestamp": "2024-05-21T08:31:29", "data": [{'
            '"ts": "2024-05-21T08:31:28.119Z", "status": "NXDOMAIN", "src_ip": "192.168.1.105", '
            '"dns_ip": "8.8.8.8", "host_domain_name": "www.heidelberg-botanik.de", "record_type": "A", "response_ip": '
            '"b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1", "size": "150b"}, {"ts": "2024-06-01T02:31:07.943Z", '
            '"status": "NXDOMAIN", "src_ip": "192.168.1.206", "dns_ip": "8.8.8.8", "host_domain_name": '
            '"www.biotech-hei.com", "record_type": "AAAA", "response_ip": "4250:5939:b4f2:b3ec:36ef:752d:b325:189b", '
            '"size": "117b"}]}'
        )
        sut.send_filtered_data()

        mock_produce_handler_instance.produce.assert_called_once_with(
            topic="produce_topic",
            data=expected_message,
            key="192.168.1.0_24",
        )

    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    @patch("src.prefilter.prefilter.ClickHouseKafkaSender")
    def test_send_without_filtered_data_with_unfiltered_data(
        self,
        mock_clickhouse,
        mock_produce_handler,
        mock_consume_handler,
        mock_logline_handler,
        mock_logger,
    ):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Prefilter(
            consume_topic="test_topic",
            produce_topics=["produce_topic"],
            relevance_function_name="no_relevance",
            validation_config={}
        )
        sut.unfiltered_data = ["message"]
        sut.filtered_data = []

        with self.assertRaises(ValueError):
            sut.send_filtered_data()

        mock_produce_handler.add_message.assert_not_called()

    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    @patch("src.prefilter.prefilter.ClickHouseKafkaSender")
    def test_send_without_data(
        self,
        mock_clickhouse,
        mock_produce_handler,
        mock_consume_handler,
        mock_logline_handler,
    ):
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Prefilter(
            consume_topic="test_topic",
            produce_topics=["produce_topic"],
            relevance_function_name="no_relevance",
            validation_config={}
        )        
        sut.unfiltered_data = []
        sut.filtered_data = []

        with self.assertRaises(ValueError):
            sut.send_filtered_data()

        mock_produce_handler.add_message.assert_not_called()


class TestClearData(unittest.TestCase):
    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    @patch("src.prefilter.prefilter.ClickHouseKafkaSender")
    def test_clear_data_with_data(
        self,
        mock_clickhouse,
        mock_produce_handler,
        mock_consume_handler,
        mock_logline_handler,
    ):
        first_entry = {
            "ts": "2024-05-21T08:31:28.119Z",
            "status": "NOERROR",
            "src_ip": "192.168.0.105",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.heidelberg-botanik.de",
            "record_type": "A",
            "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
            "size": "150b",
        }
        second_entry = {
            "ts": "2024-06-01T02:31:07.943Z",
            "status": "NXDOMAIN",
            "src_ip": "192.168.1.206",
            "dns_ip": "8.8.8.8",
            "host_domain_name": "www.biotech-hei.com",
            "record_type": "AAAA",
            "response_ip": "4250:5939:b4f2:b3ec:36ef:752d:b325:189b",
            "size": "117b",
        }

        sut = Prefilter(
            consume_topic="test_topic",
            produce_topics=["produce_topic"],
            relevance_function_name="no_relevance",
            validation_config={}
        )
        sut.unfiltered_data = [first_entry, second_entry]
        sut.filtered_data = [second_entry]
        sut.clear_data()

        self.assertEqual([], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)

    @patch("src.prefilter.prefilter.LoglineHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaConsumeHandler")
    @patch("src.prefilter.prefilter.ExactlyOnceKafkaProduceHandler")
    @patch("src.prefilter.prefilter.ClickHouseKafkaSender")
    def test_clear_data_without_data(
        self,
        mock_clickhouse,
        mock_produce_handler,
        mock_consume_handler,
        mock_logline_handler,
    ):
        sut = Prefilter(
            consume_topic="test_topic",
            produce_topics=["produce_topic"],
            relevance_function_name="no_relevance",
            validation_config={}
        )
        sut.unfiltered_data = []
        sut.filtered_data = []
        sut.clear_data()

        self.assertEqual([], sut.unfiltered_data)
        self.assertEqual([], sut.filtered_data)


class TestMainFunction(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.pf = [
            {
            "name": "dga_filter",
            "relevance_method": "no_relevance_check",
            "collector_name": "dga_collector"
            }
        ]
# Todo: test the start method instead!  and their submethods as well!
# TODO: check_data_relevance_using_rules needs to be updated so that relevance checks are executed properly!

    # @patch("src.prefilter.prefilter.logger")
    # @patch("src.prefilter.prefilter.Prefilter")
    # @patch("asyncio.create_task")  
    # @patch("asyncio.run")
    # async def test_main_normal_flow(self,mock_asyncio_run, mock_asyncio_create_task, mock_prefilter_cls, mock_logger):
    #     # Arrange
    #     mock_prefilter_instance = MagicMock()
    #     mock_prefilter_instance.start = AsyncMock()
    #     mock_prefilter_instance.clear_data = AsyncMock()
    #     mock_prefilter_instance.get_and_fill_data = AsyncMock()
    #     mock_prefilter_instance.filter_by_error = AsyncMock()

    #     mock_prefilter_cls.return_value = mock_prefilter_instance
    #     mock_asyncio_create_task.side_effect = lambda coro: coro
    #     # Act
    #     with patch("src.prefilter.prefilter.PREFILTERS", self.pf):
    #         await main()
    #     # Assert
    #     mock_prefilter_instance.get_and_fill_data.assert_called()
    #     mock_prefilter_instance.filter_by_error.assert_called()
    #     mock_prefilter_instance.send_filtered_data.assert_called()
    #     mock_prefilter_instance.clear_data.assert_called()

    # @patch("src.prefilter.prefilter.logger")
    # @patch("src.prefilter.prefilter.Prefilter")
    # @patch("asyncio.create_task")  
    # @patch("asyncio.run")
    # async def test_main_ioerror(self, mock_asyncio_run, mock_asyncio_create_task, mock_prefilter_cls, mock_logger):
    #     mock_prefilter_instance = MagicMock()
    #     mock_prefilter_instance.start = AsyncMock()
    #     mock_prefilter_instance.clear_data = AsyncMock()
    #     mock_prefilter_cls.return_value = mock_prefilter_instance
    #     mock_prefilter_instance.get_and_fill_data.side_effect = IOError
    #     mock_asyncio_create_task.side_effect = lambda coro: coro
    #     # Act and Assert
    #     with self.assertRaises(IOError):
    #         with patch("src.prefilter.prefilter.PREFILTERS", self.pf):
    #             await main()
    #     mock_prefilter_instance.clear_data.assert_called()

    @patch("src.prefilter.prefilter.logger")
    @patch("src.prefilter.prefilter.Prefilter")
    @patch("asyncio.create_task")  
    @patch("asyncio.run")
    async def test_main_normal_flow(self, mock_asyncio_run, mock_asyncio_create_task, mock_prefilter_cls, mock_logger):
        # Arrange
        mock_prefilter_instance = MagicMock()
        mock_prefilter_instance.start = AsyncMock()
        mock_prefilter_cls.return_value = mock_prefilter_instance
        mock_asyncio_create_task.side_effect = lambda coro: coro
        
        with patch("src.prefilter.prefilter.PREFILTERS", self.pf):
            await main()

        mock_prefilter_instance.start.assert_called_once()
        
    # @patch("src.prefilter.prefilter.logger")
    # @patch("src.prefilter.prefilter.Prefilter")
    # @patch("asyncio.create_task")  
    # @patch("asyncio.run")
    # async def test_main_kafka_message_fetch_exception(
    #     self, mock_asyncio_run, mock_asyncio_create_task, mock_prefilter_cls, mock_logger
    # ):
    #     mock_prefilter_instance = MagicMock()
    #     mock_prefilter_instance.clear_data = AsyncMock()
    #     mock_prefilter_cls.return_value = mock_prefilter_instance
    #     mock_prefilter_instance.get_and_fill_data.side_effect = ValueError
    #     mock_asyncio_create_task.side_effect = lambda coro: coro
    #     # Act
    #     with patch("src.prefilter.prefilter.PREFILTERS", self.pf):
    #         await main()
            
    #     # Assert
    #     mock_prefilter_instance.clear_data.assert_called()

    # @patch("src.prefilter.prefilter.logger")
    # @patch("src.prefilter.prefilter.Prefilter")
    # async def test_main_keyboard_interrupt(self, mock_prefilter, mock_logger):
    #     # Arrange
    #     mock_prefilter_instance = mock_prefilter.return_value
    #     mock_prefilter_instance.clear_data.return_value = MagicMock()
    #     mock_prefilter_instance.get_and_fill_data.side_effect = KeyboardInterrupt

    #     # Act
    #     main()

    #     # Assert
    #     mock_prefilter_instance.clear_data.assert_called()


if __name__ == "__main__":
    unittest.main()
