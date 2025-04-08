import datetime
import unittest
import uuid
from unittest.mock import patch, AsyncMock, Mock, mock_open

import marshmallow_dataclass

from src.base.data_classes.clickhouse_connectors import ServerLogs
from src.monitoring.monitoring_agent import CREATE_TABLES_DIRECTORY, main
from src.monitoring.monitoring_agent import MonitoringAgent, prepare_all_tables


class TestPrepareAllTables(unittest.TestCase):
    @patch("os.listdir", return_value=["table1.sql", "table2.sql", "not_sql.txt"])
    @patch("builtins.open", new_callable=mock_open, read_data="CREATE TABLE test;")
    @patch("clickhouse_connect.get_client")
    def test_prepare_all_tables(self, mock_get_client, mock_open_file, mock_listdir):
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value.__enter__.return_value = mock_client

        # Act
        prepare_all_tables()

        # Assert
        mock_listdir.assert_called_once_with(CREATE_TABLES_DIRECTORY)
        self.assertEqual(mock_open_file.call_count, 2)
        mock_client.command.assert_called_with("CREATE TABLE test;")
        self.assertEqual(mock_client.command.call_count, 2)

    @patch("os.listdir", return_value=["table1.sql"])
    @patch("builtins.open", new_callable=mock_open, read_data="CREATE TABLE test;")
    @patch("clickhouse_connect.get_client")
    def test_prepare_all_tables_with_exception(
        self, mock_get_client, mock_open_file, mock_listdir
    ):
        # Arrange
        mock_client = Mock()
        mock_get_client.return_value.__enter__.return_value = mock_client

        mock_client.command.side_effect = Exception("Simulated Error")

        # Act
        with self.assertRaises(Exception) as context:
            prepare_all_tables()

        # Assert
        self.assertEqual(str(context.exception), "Simulated Error")


class TestInit(unittest.TestCase):
    def test_successful(self):
        # Act
        with (
            patch(
                "src.monitoring.monitoring_agent.SimpleKafkaConsumeHandler"
            ) as mock_simple_kafka_consume_handler,
            patch(
                "src.monitoring.monitoring_agent.ClickHouseBatchSender"
            ) as mock_clickhouse_batch_sender,
        ):
            sut = MonitoringAgent()

        # Assert
        self.assertTrue(
            isinstance(sut.table_names, list)
            and all(isinstance(e, str) for e in sut.table_names)
        )
        self.assertTrue(all(e.startswith("clickhouse_") for e in sut.topics))
        self.assertIsNotNone(sut.kafka_consumer)
        self.assertIsNotNone(sut.batch_sender)
        mock_simple_kafka_consume_handler.assert_called_once_with(sut.topics)
        mock_clickhouse_batch_sender.assert_called_once()


class TestStart(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        with (
            patch("src.monitoring.monitoring_agent.SimpleKafkaConsumeHandler"),
            patch("src.monitoring.monitoring_agent.ClickHouseBatchSender"),
        ):
            self.sut = MonitoringAgent()

    async def test_successful(self):
        # Arrange
        data_schema = marshmallow_dataclass.class_schema(ServerLogs)()
        fixed_id = uuid.UUID("35871c8c-ff72-44ad-a9b7-4f02cf92d484")
        timestamp_in = datetime.datetime(2025, 4, 3, 12, 32, 7, 264410)
        value = data_schema.dumps(
            {
                "message_id": fixed_id,
                "timestamp_in": timestamp_in,
                "message_text": "test_text",
            }
        )
        self.sut.kafka_consumer.consume.return_value = (
            "test_key",
            value,
            "clickhouse_server_logs",
        )

        with patch(
            "src.monitoring.monitoring_agent.asyncio.get_running_loop"
        ) as mock_get_running_loop:
            mock_loop = AsyncMock()
            mock_get_running_loop.return_value = mock_loop

            mock_loop.run_in_executor.side_effect = [
                ("test_key", value, "clickhouse_server_logs"),
                KeyboardInterrupt(),
            ]

            # Act
            await self.sut.start()

        # Assert
        self.sut.batch_sender.add.assert_called_once_with(
            "server_logs",
            {
                "message_id": fixed_id,
                "timestamp_in": timestamp_in,
                "message_text": "test_text",
            },
        )


class TestMain(unittest.TestCase):
    @patch("src.monitoring.monitoring_agent.MonitoringAgent")
    @patch("asyncio.run")
    def test_main(self, mock_asyncio_run, mock_monitoring_agent):
        # Arrange
        mock_agent_instance = Mock()
        mock_monitoring_agent.return_value = mock_agent_instance

        # Act
        main()

        # Assert
        mock_monitoring_agent.assert_called_once()
        mock_asyncio_run.assert_called_once_with(mock_agent_instance.start())


if __name__ == "__main__":
    unittest.main()
