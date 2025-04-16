import asyncio
import datetime
import ipaddress
import unittest
import uuid
from unittest.mock import MagicMock, patch, AsyncMock, Mock

from src.logcollector.collector import LogCollector, main


class TestInit(unittest.TestCase):
    @patch("src.logcollector.collector.CONSUME_TOPIC", "test_topic")
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

        sut = LogCollector()

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
        self.sut = LogCollector()

    async def test_start_successful_execution(self):
        # Arrange
        self.sut.fetch = AsyncMock()

        async def mock_gather(*args, **kwargs):
            return None

        with patch(
            "src.logcollector.collector.asyncio.gather", side_effect=mock_gather
        ) as mock:
            # Act
            await self.sut.start()

            # Assert
            mock.assert_called_once()
            self.sut.fetch.assert_called_once()

    async def test_start_handles_keyboard_interrupt(self):
        # Arrange
        self.sut.fetch = AsyncMock()

        async def mock_gather(*args, **kwargs):
            raise KeyboardInterrupt

        with patch(
            "src.logcollector.collector.asyncio.gather", side_effect=mock_gather
        ) as mock:
            # Act
            await self.sut.start()

            # Assert
            mock.assert_called_once()
            self.sut.fetch.assert_called_once()


class TestFetch(unittest.IsolatedAsyncioTestCase):
    @patch("src.logcollector.collector.LoglineHandler")
    @patch("src.logcollector.collector.BufferedBatchSender")
    @patch("src.logcollector.collector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    async def asyncSetUp(
        self,
        mock_clickhouse,
        mock_kafka_handler,
        mock_batch_sender,
        mock_logline_handler,
    ):
        self.sut = LogCollector()
        self.sut.kafka_consume_handler = AsyncMock()

    @patch("src.logcollector.collector.LogCollector.send")
    @patch("src.logcollector.collector.logger")
    @patch("asyncio.get_running_loop")
    @patch("src.logcollector.collector.ClickHouseKafkaSender")
    async def test_handle_kafka_inputs(
        self, mock_clickhouse, mock_get_running_loop, mock_logger, mock_send
    ):
        mock_send_instance = AsyncMock()
        mock_send.return_value = mock_send_instance
        mock_loop = AsyncMock()
        mock_get_running_loop.return_value = mock_loop
        self.sut.kafka_consume_handler.consume.return_value = (
            "key1",
            "value1",
            "topic1",
        )

        mock_loop.run_in_executor.side_effect = [
            ("key1", "value1", "topic1"),
            asyncio.CancelledError(),
        ]

        with self.assertRaises(asyncio.CancelledError):
            await self.sut.fetch()

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
            self.sut = LogCollector()

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
            "timestamp": str(timestamp),
            "status_code": "test_status",
            "client_ip": "192.168.3.141",
            "record_type": "test_record_type",
        }

        # Act
        with (
            patch(
                "src.logcollector.collector.TIMESTAMP_FORMAT", "%Y-%m-%d %H:%M:%S.%f"
            ),
            patch("src.logcollector.collector.IPV4_PREFIX_LENGTH", 24),
            patch(
                "src.logcollector.collector.uuid.uuid4",
                return_value=uuid.UUID("da3aec7f-b355-4a2c-a2f4-2066d49431a5"),
            ),
        ):
            self.sut.send(timestamp_in=timestamp, message=message)

        # Assert
        self.sut.batch_handler.add_message.assert_called_once_with(
            "192.168.3.0_24",
            '{"timestamp": "2026-02-14 16:38:06.184006", "status_code": "test_status", "client_ip": "192.168.3.141", "record_type": "test_record_type", "logline_id": "da3aec7f-b355-4a2c-a2f4-2066d49431a5"}',
        )


class TestGetSubnetId(unittest.TestCase):
    @patch("src.logcollector.collector.IPV4_PREFIX_LENGTH", 24)
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
        # Arrange
        test_address = ipaddress.IPv4Address("192.168.1.1")
        expected_result = f"192.168.1.0_24"
        sut = LogCollector()

        # Act
        result = sut._get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.IPV4_PREFIX_LENGTH", 24)
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
        # Arrange
        test_address = ipaddress.IPv4Address("0.0.0.0")
        expected_result = f"0.0.0.0_24"
        sut = LogCollector()

        # Act
        result = sut._get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.IPV4_PREFIX_LENGTH", 23)
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
        # Arrange
        test_address = ipaddress.IPv4Address("255.255.255.255")
        expected_result = f"255.255.254.0_23"
        sut = LogCollector()

        # Act
        result = sut._get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.IPV6_PREFIX_LENGTH", 64)
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
        # Arrange
        test_address = ipaddress.IPv6Address("2001:db8:85a3:1234:5678:8a2e:0370:7334")
        expected_result = f"2001:db8:85a3:1234::_64"
        sut = LogCollector()

        # Act
        result = sut._get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.IPV6_PREFIX_LENGTH", 64)
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
        sut = LogCollector()

        # Act
        result = sut._get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.IPV6_PREFIX_LENGTH", 48)
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
        sut = LogCollector()

        # Act
        result = sut._get_subnet_id(test_address)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("src.logcollector.collector.IPV4_PREFIX_LENGTH", 24)
    @patch("src.logcollector.collector.IPV6_PREFIX_LENGTH", 48)
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
        sut = LogCollector()

        # Act & Assert
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            sut._get_subnet_id(test_address)

    @patch("src.logcollector.collector.IPV4_PREFIX_LENGTH", 24)
    @patch("src.logcollector.collector.IPV6_PREFIX_LENGTH", 48)
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
        sut = LogCollector()

        # Act & Assert
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            sut._get_subnet_id(test_address)


class TestMain(unittest.TestCase):
    @patch("src.logcollector.collector.logger")
    @patch("src.logcollector.collector.LogCollector")
    @patch("asyncio.run")
    def test_main(self, mock_asyncio_run, mock_instance, mock_logger):
        # Arrange
        mock_instance_obj = MagicMock()
        mock_instance.return_value = mock_instance_obj

        # Act
        main()

        # Assert
        mock_instance.assert_called_once()
        mock_asyncio_run.assert_called_once_with(mock_instance_obj.start())


if __name__ == "__main__":
    unittest.main()
