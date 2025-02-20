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
    @patch("src.monitoring.monitoring_agent.ServerLogsConnector")
    @patch("src.monitoring.monitoring_agent.ServerLogsTimestampsConnector")
    @patch("src.monitoring.monitoring_agent.FailedDNSLoglinesConnector")
    @patch("src.monitoring.monitoring_agent.LoglineToBatchesConnector")
    @patch("src.monitoring.monitoring_agent.DNSLoglinesConnector")
    @patch("src.monitoring.monitoring_agent.LoglineTimestampsConnector")
    @patch("src.monitoring.monitoring_agent.BatchTimestampsConnector")
    @patch("src.monitoring.monitoring_agent.SuspiciousBatchesToBatchConnector")
    @patch("src.monitoring.monitoring_agent.SuspiciousBatchTimestampsConnector")
    @patch("src.monitoring.monitoring_agent.AlertsConnector")
    @patch("src.monitoring.monitoring_agent.FillLevelsConnector")
    @patch("src.monitoring.monitoring_agent.SimpleKafkaConsumeHandler")
    def test_init(
        self,
        mock_kafka_consumer,
        mock_fill_levels,
        mock_alerts,
        mock_suspicious_batch_timestamps,
        mock_suspicious_batches_to_batch,
        mock_batch_timestamps,
        mock_logline_timestamps,
        mock_dns_loglines,
        mock_logline_to_batches,
        mock_failed_dns_loglines,
        mock_server_logs_timestamps,
        mock_server_logs,
    ):
        # Arrange
        expected_topics = [
            "clickhouse_server_logs",
            "clickhouse_server_logs_timestamps",
            "clickhouse_failed_dns_loglines",
            "clickhouse_logline_to_batches",
            "clickhouse_dns_loglines",
            "clickhouse_logline_timestamps",
            "clickhouse_batch_timestamps",
            "clickhouse_suspicious_batches_to_batch",
            "clickhouse_suspicious_batch_timestamps",
            "clickhouse_alerts",
            "clickhouse_fill_levels",
        ]

        # Act
        sut = MonitoringAgent()

        # Assert
        self.assertEqual(
            expected_topics,
            sut.topics,
        )
        mock_kafka_consumer.assert_called_once_with(expected_topics)


class TestStart(unittest.IsolatedAsyncioTestCase):
    @patch("src.monitoring.monitoring_agent.ServerLogsConnector")
    @patch("src.monitoring.monitoring_agent.ServerLogsTimestampsConnector")
    @patch("src.monitoring.monitoring_agent.FailedDNSLoglinesConnector")
    @patch("src.monitoring.monitoring_agent.LoglineToBatchesConnector")
    @patch("src.monitoring.monitoring_agent.DNSLoglinesConnector")
    @patch("src.monitoring.monitoring_agent.LoglineTimestampsConnector")
    @patch("src.monitoring.monitoring_agent.BatchTimestampsConnector")
    @patch("src.monitoring.monitoring_agent.SuspiciousBatchesToBatchConnector")
    @patch("src.monitoring.monitoring_agent.SuspiciousBatchTimestampsConnector")
    @patch("src.monitoring.monitoring_agent.AlertsConnector")
    @patch("src.monitoring.monitoring_agent.FillLevelsConnector")
    @patch("src.monitoring.monitoring_agent.logger")
    @patch("src.monitoring.monitoring_agent.SimpleKafkaConsumeHandler")
    @patch("asyncio.get_running_loop")
    async def test_handle_kafka_inputs(
        self,
        mock_get_running_loop,
        mock_kafka_consume,
        mock_logger,
        mock_fill_levels,
        mock_alerts,
        mock_suspicious_batch_timestamps,
        mock_suspicious_batches_to_batch,
        mock_batch_timestamps,
        mock_logline_timestamps,
        mock_dns_loglines,
        mock_logline_to_batches,
        mock_failed_dns_loglines,
        mock_server_logs_timestamps,
        mock_server_logs,
    ):
        # Arrange
        sut = MonitoringAgent()
        sut.connectors["server_logs"] = Mock()

        data_schema = marshmallow_dataclass.class_schema(ServerLogs)()
        fixed_id = uuid.uuid4()
        timestamp_in = datetime.datetime.now()
        value = data_schema.dumps(
            {
                "message_id": fixed_id,
                "timestamp_in": timestamp_in,
                "message_text": "test_text",
            }
        )

        mock_loop = AsyncMock()
        mock_get_running_loop.return_value = mock_loop
        sut.kafka_consumer.consume.return_value = (
            "key1",
            value,
            "clickhouse_server_logs",
        )
        mock_loop.run_in_executor.side_effect = [
            ("key1", value, "clickhouse_server_logs"),
            KeyboardInterrupt(),
        ]

        # Act and Assert
        await sut.start()

        sut.connectors["server_logs"].insert.assert_called_once_with(
            message_id=fixed_id,
            timestamp_in=timestamp_in,
            message_text="test_text",
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
