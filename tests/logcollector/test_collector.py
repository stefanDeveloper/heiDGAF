import asyncio
import datetime
import ipaddress
import unittest
import uuid
from unittest.mock import MagicMock, patch, AsyncMock, Mock

from src.logcollector.collector import LogCollector, main
from src.base.utils import setup_config


class TestInit(unittest.TestCase):
    @patch("src.logcollector.collector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.logcollector.collector.BufferedBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def test_valid_init(
        self,
        mock_clickhouse,
        mock_logline_handler,
        mock_batch_handler,
        mock_kafka_handler,
    ):
        mock_batch_handler_instance = MagicMock()
        mock_logline_handler_instance = MagicMock()
        mock_kafka_handler_instance = MagicMock()
        mock_batch_handler.return_value = mock_batch_handler_instance
        mock_logline_handler.return_value = mock_logline_handler_instance
        mock_kafka_handler.return_value = mock_kafka_handler_instance

        sut = LogCollector(
            collector_name="test-collector",
            consume_topic="test_topic",
            produce_topics=["produce-topic"],
            protocol="dns",
            validation_config={}           
        )

        self.assertEqual(mock_batch_handler_instance, sut.batch_handler)
        self.assertEqual(mock_logline_handler_instance, sut.logline_handler)
        self.assertEqual(mock_kafka_handler_instance, sut.kafka_consume_handler)

        mock_batch_handler.assert_called_once()
        mock_logline_handler.assert_called_once()
        mock_kafka_handler.assert_called_once_with("test_topic")


class TestStart(unittest.IsolatedAsyncioTestCase):
    @patch("src.logcollector.collector.logger")
    @patch("src.logcollector.collector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.logcollector.collector.BufferedBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def setUp(
        self,
        mock_clickhouse,
        mock_logline_handler,
        mock_batch_handler,
        mock_kafka_consume_handler,
        mock_logger,
    ):
        self.sut = LogCollector(
            collector_name="my-collector",
            consume_topic="consume-topic",
            produce_topics=["produce-topic"],
            protocol="dns",
            validation_config={}
        )

    async def test_start_successful_execution(self):
        # Arrange
        self.sut.fetch = MagicMock()
        await self.sut.start()
        self.sut.fetch.assert_called_once()
class _StopFetching(RuntimeError):
    """Raised inside the test to break the infinite fetch loop."""

class TestFetch(unittest.TestCase):
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.BufferedBatchSender")
    @patch("src.logcollector.collector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.logcollector.collector.LogCollector.send")
    @patch("src.logcollector.collector.logger")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def test_handle_kafka_inputs(
        self, mock_clickhouse, mock_logger, mock_send, mock_kafka_consume, mock_batch_sender, mock_logline_handler,

    ):
        mock_consume_handler = MagicMock()
        mock_consume_handler.consume.side_effect = [
            ("key1", "value1", "topic1"),
            _StopFetching(),
        ]
        mock_kafka_consume.return_value = mock_consume_handler
        mock_send_instance = MagicMock()
        mock_send.return_value = mock_send_instance
        self.sut = LogCollector(
            collector_name="my-collector",
            consume_topic="consume-topic",
            produce_topics=["produce-topic"],
            protocol="dns",
            validation_config={}
        )        
        original_fetch = self.sut.fetch
        def fetch_wrapper(*args, **kwargs):
            try:
                original_fetch(*args, **kwargs)
            except _StopFetching:
                return
        with patch.object(self.sut, "fetch", new=fetch_wrapper):
            self.sut.fetch()

        mock_send.assert_called_once()

class TestSend(unittest.TestCase):
    def setUp(self):
        with (
            patch("src.logcollector.collector.asyncio.Queue"),
            patch("src.logcollector.collector.BufferedBatchSender"),
            patch("src.logcollector.collector.LoglineHandler"),
            patch("src.logcollector.collector.ExactlyOnceKafkaConsumeHandler"),
            patch("src.logcollector.collector.ClickHouseKafkaSender"),
        ):
            self.sut = LogCollector(
                collector_name="my-collector",
                consume_topic="consume-topic",
                produce_topics=["produce-topic"],
                protocol="dns",
                validation_config={}
            )
    def test_valid_logline(self):
        timestamp = datetime.datetime(2026, 2, 14, 16, 38, 6, 184006)
        message = "test_message"

        # Arrange
        mock_logline_handler = Mock()
        self.sut.logline_handler = mock_logline_handler.return_value
        self.sut.logline_handler.validate_logline_and_get_fields_as_json.side_effect = [
            ValueError
        ]

        # Act
        self.sut.send(timestamp_in=timestamp, message=message)

        # Assert
        self.sut.batch_handler.add_message.assert_not_called()

    def test_invalid_logline(self):
        timestamp = datetime.datetime(2026, 2, 14, 16, 38, 6, 184006)
        message = "test_message"

        # Arrange
        mock_logline_handler = Mock()
        self.sut.logline_handler = mock_logline_handler.return_value
        self.sut.logline_handler.validate_logline_and_get_fields_as_json.return_value = {
            "ts": str(timestamp),
            "status_code": "test_status",
            "src_ip": "192.168.3.141",
            "record_type": "test_record_type",
        }

        # Act
        with (
            patch(
                "src.logcollector.collector.uuid.uuid4",
                return_value=uuid.UUID("da3aec7f-b355-4a2c-a2f4-2066d49431a5"),
            ),
        ):
            self.sut.send(timestamp_in=timestamp, message=message)

        # Assert
        self.sut.batch_handler.add_message.assert_called_once_with(
            "192.168.3.0_24",
            '{"ts": "2026-02-14 16:38:06.184006", "status_code": "test_status", "src_ip": "192.168.3.141", "record_type": "test_record_type", "logline_id": "da3aec7f-b355-4a2c-a2f4-2066d49431a5"}',
        )


class TestGetSubnetId(unittest.TestCase):
    @patch("src.logcollector.collector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.logcollector.collector.BufferedBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def test_get_subnet_id_ipv4(
        self,
        mock_clickhouse,
        mock_logline_handler,
        mock_batch_handler,
        mock_kafka_handler,
    ):
        test_address = ipaddress.IPv4Address("192.168.1.1")
        expected_result = f"192.168.1.0_24"
        sut = LogCollector(
                collector_name="my-collector",
                consume_topic="consume-topic",
                produce_topics=["produce-topic"],
                protocol="dns",
                validation_config={}
        )
        sut.batch_configuration = {
            "batch_size": 100,
            "batch_timeout": 5.9,
            "subnet_id": {
                "ipv4_prefix_length": 24,
                "ipv6_prefix_length": 64
            }  
        }
        # Act
        result = sut._get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.logcollector.collector.BufferedBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def test_get_subnet_id_ipv4_zero(
        self,
        mock_clickhouse,
        mock_logline_handler,
        mock_batch_handler,
        mock_kafka_handler,
    ):
        test_address = ipaddress.IPv4Address("0.0.0.0")
        expected_result = f"0.0.0.0_24"
        sut = LogCollector(
                collector_name="my-collector",
                consume_topic="consume-topic",
                produce_topics=["produce-topic"],
                protocol="dns",
                validation_config={}
        )
        sut.batch_configuration = {
            "batch_size": 100,
            "batch_timeout": 5.9,
            "subnet_id": {
                "ipv4_prefix_length": 24,
                "ipv6_prefix_length": 64
            }   
        }        
        # Act
        result = sut._get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.logcollector.collector.BufferedBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def test_get_subnet_id_ipv4_max(
        self,
        mock_clickhouse,
        mock_logline_handler,
        mock_batch_handler,
        mock_kafka_handler,
    ):
        test_address = ipaddress.IPv4Address("255.255.255.255")
        expected_result = f"255.255.254.0_23"
        sut = LogCollector(
                collector_name="my-collector",
                consume_topic="consume-topic",
                produce_topics=["produce-topic"],
                protocol="dns",
                validation_config={}
        )
        sut.batch_configuration = {
            "batch_size": 100,
            "batch_timeout": 5.9,
            "subnet_id": {
                "ipv4_prefix_length": 23,
                "ipv6_prefix_length": 64
            }             
        }
        # Act
        result = sut._get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.logcollector.collector.BufferedBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def test_get_subnet_id_ipv6(
        self,
        mock_clickhouse,
        mock_logline_handler,
        mock_batch_handler,
        mock_kafka_handler,
    ):
        test_address = ipaddress.IPv6Address("2001:db8:85a3:1234:5678:8a2e:0370:7334")
        expected_result = f"2001:db8:85a3:1234::_64"
        sut = LogCollector(
                collector_name="my-collector",
                consume_topic="consume-topic",
                produce_topics=["produce-topic"],
                protocol="dns",
                validation_config={}
        )
        # Act
        sut.batch_configuration = {
            "batch_size": 100,
            "batch_timeout": 5.9,
            "subnet_id": {
                "ipv4_prefix_length": 24,
                "ipv6_prefix_length": 64
            }            
        }
        result = sut._get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.logcollector.collector.BufferedBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def test_get_subnet_id_ipv6_zero(
        self,
        mock_clickhouse,
        mock_logline_handler,
        mock_batch_handler,
        mock_kafka_handler,
    ):
        # Arrange
        test_address = ipaddress.IPv6Address("::")
        expected_result = f"::_64"
        sut = LogCollector(
                collector_name="my-collector",
                consume_topic="consume-topic",
                produce_topics=["produce-topic"],
                protocol="dns",
                validation_config={}
        )
        sut.batch_configuration = {
            "batch_size": 100,
            "batch_timeout": 5.9,
            "subnet_id": {
                "ipv4_prefix_length": 24,
                "ipv6_prefix_length": 64
            }    
        }
        # Act
        result = sut._get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.logcollector.collector.BufferedBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def test_get_subnet_id_ipv6_max(
        self,
        mock_clickhouse,
        mock_logline_handler,
        mock_batch_handler,
        mock_kafka_handler,
    ):
        # Arrange
        test_address = ipaddress.IPv6Address("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff")
        expected_result = f"ffff:ffff:ffff::_48"
        sut = LogCollector(
                collector_name="my-collector",
                consume_topic="consume-topic",
                produce_topics=["produce-topic"],
                protocol="dns",
                validation_config={}
        )
        sut.batch_configuration = {
            "batch_size": 100,
            "batch_timeout": 5.9,
            "subnet_id": {
                "ipv4_prefix_length": 24,
                "ipv6_prefix_length": 48
            }
        }    
        # Act
        result = sut._get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)
        
    @patch("src.logcollector.collector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.logcollector.collector.BufferedBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def test_get_subnet_id_unsupported_type(
        self,
        mock_clickhouse,
        mock_logline_handler,
        mock_batch_handler,
        mock_kafka_handler,
    ):
        # Arrange
        test_address = "192.168.1.1"  # String instead of IPv4Address or IPv6Address
        sut = LogCollector(
                collector_name="my-collector",
                consume_topic="consume-topic",
                produce_topics=["produce-topic"],
                protocol="dns",
                validation_config={}
        )
        sut.batch_configuration = {
            "batch_size": 100,
            "batch_timeout": 5.9,
            "subnet_id": {
                "ipv4_prefix_length": 24,
                "ipv6_prefix_length": 48
            }    
        }
        # Act & Assert
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            sut._get_subnet_id(test_address)
            
    @patch("src.logcollector.collector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.logcollector.collector.BufferedBatchSender")
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    def test_get_subnet_id_none(
        self,
        mock_clickhouse,
        mock_logline_handler,
        mock_batch_handler,
        mock_kafka_handler,
    ):
        # Arrange
        test_address = None
        sut = LogCollector(
                collector_name="my-collector",
                consume_topic="consume-topic",
                produce_topics=["produce-topic"],
                protocol="dns",
                validation_config={}
        )
        sut.batch_configuration = {
            "batch_size": 100,
            "batch_timeout": 5.9,
            "subnet_id": {
                "ipv4_prefix_length": 24,
                "ipv6_prefix_length": 48
            }    
        }

        # Act & Assert
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            sut._get_subnet_id(test_address)

class TestMain(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.cs = [
            {   
                "name": "test_collector", 
                "protocol_base": "dns",
                "required_log_information": [
                    [ "ts", "Timestamp", "%Y-%m-%dT%H:%M:%S" ],
                    [ "domain_name", "RegEx", '^(?=.{1,253}$)((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,63}$' ],
                    [ "src_ip", "IpAddress" ]
                ]
            }
        ]
    
    @patch("src.logcollector.collector.logger")
    @patch("src.logcollector.collector.LogCollector")
    @patch("asyncio.create_task")  
    @patch("asyncio.run")
    async def test_main(self, mock_asyncio_run, mock_asyncio_create_task, mock_instance, mock_logger):
        # Arrange
        
        mock_instance_obj = MagicMock()
        mock_instance.return_value = mock_instance_obj
        mock_instance_obj.start = AsyncMock()
        mock_asyncio_create_task.side_effect = lambda coro: coro
        
        with patch("src.logcollector.collector.COLLECTORS", self.cs):
            await main()

        mock_instance_obj.start.assert_called_once()
        args, kwargs = mock_asyncio_create_task.call_args_list[0]
        expected_call = args[0]
        mock_asyncio_create_task.assert_called_once_with(expected_call)


if __name__ == "__main__":
    unittest.main()
