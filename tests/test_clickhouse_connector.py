import json
import unittest
from unittest.mock import patch, MagicMock, mock_open

from src.monitoring.clickhouse_connector import *


class TestClickHouseConnector(unittest.TestCase):
    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_init(self, mock_clickhouse_batch_sender):
        # Arrange
        mock_clickhouse_batch_sender_instance = MagicMock()
        mock_clickhouse_batch_sender.return_value = (
            mock_clickhouse_batch_sender_instance
        )

        table_name = "test_table"
        column_names = ["col_1", "col_2", "col_3"]

        # Act
        sut = ClickHouseConnector(table_name, column_names)

        # Assert
        self.assertEqual(table_name, sut._table_name)
        self.assertEqual(column_names, sut._column_names)
        self.assertEqual(mock_clickhouse_batch_sender_instance, sut._batch_sender)

        mock_clickhouse_batch_sender.assert_called_once_with(
            table_name=table_name,
            column_names=column_names,
        )

    @patch("src.monitoring.clickhouse_connector.os.path.join")
    @patch(
        "src.monitoring.clickhouse_connector.open",
        new_callable=mock_open,
        read_data="CREATE TABLE test;",
    )
    @patch("src.monitoring.clickhouse_connector.clickhouse_connect.get_client")
    def test_prepare_table_success(
        self, mock_get_client, mock_open_file, mock_path_join
    ):
        # Arrange
        mock_client = MagicMock()
        mock_get_client.return_value.__enter__.return_value = mock_client
        mock_path_join.return_value = "/fake/path/test_table.sql"

        sut = ClickHouseConnector("test_table", ["col_1", "col_2", "col_3"])

        # Act
        sut.prepare_table()

        # Assert
        mock_open_file.assert_called_once_with("/fake/path/test_table.sql", "r")
        mock_client.command.assert_called_once_with("CREATE TABLE test;")

    @patch("src.monitoring.clickhouse_connector.os.path.join")
    @patch(
        "src.monitoring.clickhouse_connector.open",
        new_callable=mock_open,
        read_data="CREATE TABLE test;",
    )
    @patch("src.monitoring.clickhouse_connector.clickhouse_connect.get_client")
    @patch("src.monitoring.clickhouse_connector.logger")
    def test_prepare_table_failure(
        self, mock_logger, mock_get_client, mock_open_file, mock_path_join
    ):
        mock_client = MagicMock()
        mock_get_client.return_value.__enter__.return_value = mock_client
        mock_path_join.return_value = "/fake/path/test_table.sql"
        mock_client.command.side_effect = Exception("Test exception")

        sut = ClickHouseConnector("test_table", ["col_1", "col_2", "col_3"])

        with self.assertRaises(Exception):
            sut.prepare_table()

    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_add_to_batch(self, mock_clickhouse_batch_sender):
        # Arrange
        mock_clickhouse_batch_sender_instance = MagicMock()
        mock_clickhouse_batch_sender.return_value = (
            mock_clickhouse_batch_sender_instance
        )

        sut = ClickHouseConnector("test_table", ["col_1", "col_2", "col_3"])

        # Act
        sut._add_to_batch("test_data")

        # Assert
        mock_clickhouse_batch_sender_instance.add.assert_called_once_with("test_data")

    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_insert(self, mock_clickhouse_batch_sender):
        # Arrange
        sut = ClickHouseConnector("test_table", ["col_1", "col_2", "col_3"])

        # Act
        sut.insert("test_data")


class TestServerLogsConnector(unittest.TestCase):
    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_init(self, mock_clickhouse_batch_sender):
        # Arrange
        mock_clickhouse_batch_sender_instance = MagicMock()
        mock_clickhouse_batch_sender.return_value = (
            mock_clickhouse_batch_sender_instance
        )

        expected_table_name = "server_logs"
        expected_column_names = [
            "message_id",
            "timestamp_in",
            "message_text",
        ]

        # Act
        sut = ServerLogsConnector()

        # Assert
        self.assertEqual(expected_table_name, sut._table_name)
        self.assertEqual(expected_column_names, sut._column_names)
        self.assertEqual(mock_clickhouse_batch_sender_instance, sut._batch_sender)

        mock_clickhouse_batch_sender.assert_called_once_with(
            table_name=expected_table_name,
            column_names=expected_column_names,
        )

    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_insert_all_given(self, mock_clickhouse_batch_sender):
        # Arrange
        message_text = "test_message_text"
        message_id = "7299539b-6215-4f6b-b39f-69335aafbeff"
        timestamp_in = "2034-12-13 12:34:12.132412"

        sut = ServerLogsConnector()

        with patch.object(sut, "_add_to_batch", MagicMock()) as mock_add_to_batch:
            # Act
            returned_value = sut.insert(
                message_text=message_text,
                message_id=message_id,
                timestamp_in=timestamp_in,
            )

            # Assert
            mock_add_to_batch.assert_called_once_with(
                [
                    uuid.UUID("7299539b-6215-4f6b-b39f-69335aafbeff"),
                    datetime.datetime(2034, 12, 13, 12, 34, 12, 132412),
                    "test_message_text",
                ]
            )
            self.assertEqual(
                uuid.UUID("7299539b-6215-4f6b-b39f-69335aafbeff"), returned_value
            )

    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_insert_none_given(self, mock_clickhouse_batch_sender):
        # Arrange
        sut = ServerLogsConnector()

        with patch.object(sut, "_add_to_batch", MagicMock()) as mock_add_to_batch:
            # Act
            sut.insert("test_message_text")

            # Assert
            mock_add_to_batch.assert_called_once()


class TestServerLogsTimestampsConnector(unittest.TestCase):
    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_init(self, mock_clickhouse_batch_sender):
        # Arrange
        mock_clickhouse_batch_sender_instance = MagicMock()
        mock_clickhouse_batch_sender.return_value = (
            mock_clickhouse_batch_sender_instance
        )

        expected_table_name = "server_logs_timestamps"
        expected_column_names = [
            "message_id",
            "event",
            "event_timestamp",
        ]

        # Act
        sut = ServerLogsTimestampsConnector()

        # Assert
        self.assertEqual(expected_table_name, sut._table_name)
        self.assertEqual(expected_column_names, sut._column_names)
        self.assertEqual(mock_clickhouse_batch_sender_instance, sut._batch_sender)

        mock_clickhouse_batch_sender.assert_called_once_with(
            table_name=expected_table_name,
            column_names=expected_column_names,
        )

    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_insert_all_given(self, mock_clickhouse_batch_sender):
        # Arrange
        message_id = "7299539b-6215-4f6b-b39f-69335aafbeff"
        event = "test_event"
        event_timestamp = "2034-12-13 12:34:12.132412"

        sut = ServerLogsTimestampsConnector()

        with patch.object(sut, "_add_to_batch", MagicMock()) as mock_add_to_batch:
            # Act
            sut.insert(
                message_id=message_id,
                event=event,
                event_timestamp=event_timestamp,
            )

            # Assert
            mock_add_to_batch.assert_called_once_with(
                [
                    uuid.UUID("7299539b-6215-4f6b-b39f-69335aafbeff"),
                    "test_event",
                    datetime.datetime(2034, 12, 13, 12, 34, 12, 132412),
                ]
            )

    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_insert_none_given(self, mock_clickhouse_batch_sender):
        # Arrange
        sut = ServerLogsTimestampsConnector()

        with patch.object(sut, "_add_to_batch", MagicMock()) as mock_add_to_batch:
            # Act
            sut.insert(
                uuid.UUID("7299539b-6215-4f6b-b39f-69335aafbeff"),
                "test_event",
            )

            # Assert
            mock_add_to_batch.assert_called_once()


class TestFailedDNSLoglinesConnector(unittest.TestCase):
    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_init(self, mock_clickhouse_batch_sender):
        # Arrange
        mock_clickhouse_batch_sender_instance = MagicMock()
        mock_clickhouse_batch_sender.return_value = (
            mock_clickhouse_batch_sender_instance
        )

        expected_table_name = "failed_dns_loglines"
        expected_column_names = [
            "message_text",
            "timestamp_in",
            "timestamp_failed",
            "reason_for_failure",
        ]

        # Act
        sut = FailedDNSLoglinesConnector()

        # Assert
        self.assertEqual(expected_table_name, sut._table_name)
        self.assertEqual(expected_column_names, sut._column_names)
        self.assertEqual(mock_clickhouse_batch_sender_instance, sut._batch_sender)

        mock_clickhouse_batch_sender.assert_called_once_with(
            table_name=expected_table_name,
            column_names=expected_column_names,
        )

    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_insert_all_given(self, mock_clickhouse_batch_sender):
        # Arrange
        message_text = "test_message_text"
        timestamp_in = "2034-12-13 12:34:12.132412"
        timestamp_failed = "2034-12-13 12:35:35.542635"
        reason_for_failure = "Wrong client_ip field"

        sut = FailedDNSLoglinesConnector()

        with patch.object(sut, "_add_to_batch", MagicMock()) as mock_add_to_batch:
            # Act
            sut.insert(
                message_text=message_text,
                timestamp_in=timestamp_in,
                timestamp_failed=timestamp_failed,
                reason_for_failure=reason_for_failure,
            )

            # Assert
            mock_add_to_batch.assert_called_once_with(
                [
                    "test_message_text",
                    datetime.datetime(2034, 12, 13, 12, 34, 12, 132412),
                    datetime.datetime(2034, 12, 13, 12, 35, 35, 542635),
                    "Wrong client_ip field",
                ]
            )

    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_insert_none_given(self, mock_clickhouse_batch_sender):
        # Arrange
        message_text = "test_message_text"
        timestamp_in = "2034-12-13 12:34:12.132412"

        sut = FailedDNSLoglinesConnector()

        with patch.object(sut, "_add_to_batch", MagicMock()) as mock_add_to_batch:
            # Act
            sut.insert(
                message_text=message_text,
                timestamp_in=datetime.datetime(2034, 12, 13, 12, 34, 12, 132412),
            )

            # Assert
            mock_add_to_batch.assert_called_once()


class TestLoglineToBatchesConnector(unittest.TestCase):
    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_init(self, mock_clickhouse_batch_sender):
        # Arrange
        mock_clickhouse_batch_sender_instance = MagicMock()
        mock_clickhouse_batch_sender.return_value = (
            mock_clickhouse_batch_sender_instance
        )

        expected_table_name = "logline_to_batches"
        expected_column_names = [
            "logline_id",
            "batch_id",
        ]

        # Act
        sut = LoglineToBatchesConnector()

        # Assert
        self.assertEqual(expected_table_name, sut._table_name)
        self.assertEqual(expected_column_names, sut._column_names)
        self.assertEqual(mock_clickhouse_batch_sender_instance, sut._batch_sender)

        mock_clickhouse_batch_sender.assert_called_once_with(
            table_name=expected_table_name,
            column_names=expected_column_names,
        )

    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_insert_all_given_as_str(self, mock_clickhouse_batch_sender):
        # Arrange
        logline_id = "7299539b-6215-4f6b-b39f-69335aafbeff"
        batch_id = "1f855c43-8a75-4b53-b6cd-4a13b89312d6"

        sut = LoglineToBatchesConnector()

        with patch.object(sut, "_add_to_batch", MagicMock()) as mock_add_to_batch:
            # Act
            sut.insert(
                logline_id=logline_id,
                batch_id=batch_id,
            )

            # Assert
            mock_add_to_batch.assert_called_once_with(
                [
                    uuid.UUID("7299539b-6215-4f6b-b39f-69335aafbeff"),
                    uuid.UUID("1f855c43-8a75-4b53-b6cd-4a13b89312d6"),
                ]
            )

    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_insert_all_given_as_uuid(self, mock_clickhouse_batch_sender):
        # Arrange
        logline_id = uuid.UUID("7299539b-6215-4f6b-b39f-69335aafbeff")
        batch_id = uuid.UUID("1f855c43-8a75-4b53-b6cd-4a13b89312d6")

        sut = LoglineToBatchesConnector()

        with patch.object(sut, "_add_to_batch", MagicMock()) as mock_add_to_batch:
            # Act
            sut.insert(
                logline_id=logline_id,
                batch_id=batch_id,
            )

            # Assert
            mock_add_to_batch.assert_called_once_with(
                [
                    uuid.UUID("7299539b-6215-4f6b-b39f-69335aafbeff"),
                    uuid.UUID("1f855c43-8a75-4b53-b6cd-4a13b89312d6"),
                ]
            )


class TestDNSLoglinesConnector(unittest.TestCase):
    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_init(self, mock_clickhouse_batch_sender):
        # Arrange
        mock_clickhouse_batch_sender_instance = MagicMock()
        mock_clickhouse_batch_sender.return_value = (
            mock_clickhouse_batch_sender_instance
        )

        expected_table_name = "dns_loglines"
        expected_column_names = [
            "logline_id",
            "subnet_id",
            "timestamp",
            "status_code",
            "client_ip",
            "record_type",
            "additional_fields",
        ]

        # Act
        sut = DNSLoglinesConnector()

        # Assert
        self.assertEqual(expected_table_name, sut._table_name)
        self.assertEqual(expected_column_names, sut._column_names)
        self.assertEqual(mock_clickhouse_batch_sender_instance, sut._batch_sender)

        mock_clickhouse_batch_sender.assert_called_once_with(
            table_name=expected_table_name,
            column_names=expected_column_names,
        )

    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_insert_all_given(self, mock_clickhouse_batch_sender):
        # Arrange
        subnet_id = "127.0.0.0_24"
        timestamp = "2034-12-13 12:34:12.132412"
        status_code = "NXDOMAIN"
        client_ip = "127.0.0.1"
        record_type = "A"
        additional_fields = json.dumps(dict(test="some_field"))

        sut = DNSLoglinesConnector()

        with patch.object(sut, "_add_to_batch", MagicMock()) as mock_add_to_batch:
            # Act
            returned_value = sut.insert(
                subnet_id=subnet_id,
                timestamp=timestamp,
                status_code=status_code,
                client_ip=client_ip,
                record_type=record_type,
                additional_fields=additional_fields,
            )

            # Assert
            mock_add_to_batch.assert_called_once()
            self.assertTrue(isinstance(returned_value, uuid.UUID))


class TestLoglineStatusConnector(unittest.TestCase):
    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_init(self, mock_clickhouse_batch_sender):
        # Arrange
        mock_clickhouse_batch_sender_instance = MagicMock()
        mock_clickhouse_batch_sender.return_value = (
            mock_clickhouse_batch_sender_instance
        )

        expected_table_name = "logline_status"
        expected_column_names = [
            "logline_id",
            "status",
            "exit_at_stage",
        ]

        # Act
        sut = LoglineStatusConnector()

        # Assert
        self.assertEqual(expected_table_name, sut._table_name)
        self.assertEqual(expected_column_names, sut._column_names)
        self.assertEqual(mock_clickhouse_batch_sender_instance, sut._batch_sender)

        mock_clickhouse_batch_sender.assert_called_once_with(
            table_name=expected_table_name,
            column_names=expected_column_names,
        )

    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_insert_all_given(self, mock_clickhouse_batch_sender):
        # Arrange
        logline_id = "7299539b-6215-4f6b-b39f-69335aafbeff"
        status = "inactive"
        exit_at_stage = "prefilter"

        sut = LoglineStatusConnector()

        with patch.object(sut, "_add_to_batch", MagicMock()) as mock_add_to_batch:
            # Act
            sut.insert(
                logline_id=logline_id,
                status=status,
                exit_at_stage=exit_at_stage,
            )

            # Assert
            mock_add_to_batch.assert_called_once_with(
                [
                    uuid.UUID("7299539b-6215-4f6b-b39f-69335aafbeff"),
                    "inactive",
                    "prefilter",
                ]
            )

    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_insert_none_given(self, mock_clickhouse_batch_sender):
        # Arrange
        logline_id = uuid.UUID("7299539b-6215-4f6b-b39f-69335aafbeff")
        status = "inactive"

        sut = LoglineStatusConnector()

        with patch.object(sut, "_add_to_batch", MagicMock()) as mock_add_to_batch:
            # Act
            sut.insert(
                logline_id=logline_id,
                status=status,
            )

            # Assert
            mock_add_to_batch.assert_called_once_with(
                [
                    uuid.UUID("7299539b-6215-4f6b-b39f-69335aafbeff"),
                    "inactive",
                    None,
                ]
            )


class TestLoglineTimestampsConnector(unittest.TestCase):
    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_init(self, mock_clickhouse_batch_sender):
        # Arrange
        mock_clickhouse_batch_sender_instance = MagicMock()
        mock_clickhouse_batch_sender.return_value = (
            mock_clickhouse_batch_sender_instance
        )

        expected_table_name = "logline_timestamps"
        expected_column_names = [
            "logline_id",
            "stage",
            "status",
            "timestamp",
        ]

        # Act
        sut = LoglineTimestampsConnector()

        # Assert
        self.assertEqual(expected_table_name, sut._table_name)
        self.assertEqual(expected_column_names, sut._column_names)
        self.assertEqual(mock_clickhouse_batch_sender_instance, sut._batch_sender)

        mock_clickhouse_batch_sender.assert_called_once_with(
            table_name=expected_table_name,
            column_names=expected_column_names,
        )

    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_insert_all_given(self, mock_clickhouse_batch_sender):
        # Arrange
        logline_id = "7299539b-6215-4f6b-b39f-69335aafbeff"
        stage = "prefilter"
        status = "prefilter_out"
        timestamp = "2034-12-13 12:35:35.542635"

        sut = LoglineTimestampsConnector()

        with patch.object(sut, "_add_to_batch", MagicMock()) as mock_add_to_batch:
            # Act
            sut.insert(
                logline_id=logline_id,
                stage=stage,
                status=status,
                timestamp=timestamp,
            )

            # Assert
            mock_add_to_batch.assert_called_once_with(
                [
                    uuid.UUID("7299539b-6215-4f6b-b39f-69335aafbeff"),
                    "prefilter",
                    "prefilter_out",
                    datetime.datetime(2034, 12, 13, 12, 35, 35, 542635),
                ]
            )

    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_insert_none_given(self, mock_clickhouse_batch_sender):
        # Arrange
        logline_id = uuid.UUID("7299539b-6215-4f6b-b39f-69335aafbeff")
        stage = "prefilter"
        status = "prefilter_out"

        sut = LoglineTimestampsConnector()

        with patch.object(sut, "_add_to_batch", MagicMock()) as mock_add_to_batch:
            # Act
            sut.insert(
                logline_id=logline_id,
                stage=stage,
                status=status,
            )

            # Assert
            mock_add_to_batch.assert_called_once()


class TestBatchStatusConnector(unittest.TestCase):
    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_init(self, mock_clickhouse_batch_sender):
        # Arrange
        mock_clickhouse_batch_sender_instance = MagicMock()
        mock_clickhouse_batch_sender.return_value = (
            mock_clickhouse_batch_sender_instance
        )

        expected_table_name = "batch_status"
        expected_column_names = [
            "batch_id",
            "status",
            "exit_at_stage",
        ]

        # Act
        sut = BatchStatusConnector()

        # Assert
        self.assertEqual(expected_table_name, sut._table_name)
        self.assertEqual(expected_column_names, sut._column_names)
        self.assertEqual(mock_clickhouse_batch_sender_instance, sut._batch_sender)

        mock_clickhouse_batch_sender.assert_called_once_with(
            table_name=expected_table_name,
            column_names=expected_column_names,
        )

    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_insert_all_given(self, mock_clickhouse_batch_sender):
        # Arrange
        batch_id = "7299539b-6215-4f6b-b39f-69335aafbeff"
        status = "inactive"
        exit_at_stage = "prefilter"

        sut = BatchStatusConnector()

        with patch.object(sut, "_add_to_batch", MagicMock()) as mock_add_to_batch:
            # Act
            sut.insert(
                batch_id=batch_id,
                status=status,
                exit_at_stage=exit_at_stage,
            )

            # Assert
            mock_add_to_batch.assert_called_once_with(
                [
                    uuid.UUID("7299539b-6215-4f6b-b39f-69335aafbeff"),
                    "inactive",
                    "prefilter",
                ]
            )

    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_insert_none_given(self, mock_clickhouse_batch_sender):
        # Arrange
        batch_id = uuid.UUID("7299539b-6215-4f6b-b39f-69335aafbeff")
        status = "inactive"

        sut = BatchStatusConnector()

        with patch.object(sut, "_add_to_batch", MagicMock()) as mock_add_to_batch:
            # Act
            sut.insert(
                batch_id=batch_id,
                status=status,
            )

            # Assert
            mock_add_to_batch.assert_called_once_with(
                [
                    uuid.UUID("7299539b-6215-4f6b-b39f-69335aafbeff"),
                    "inactive",
                    None,
                ]
            )


class TestBatchTimestampsConnector(unittest.TestCase):
    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_init(self, mock_clickhouse_batch_sender):
        # Arrange
        mock_clickhouse_batch_sender_instance = MagicMock()
        mock_clickhouse_batch_sender.return_value = (
            mock_clickhouse_batch_sender_instance
        )

        expected_table_name = "batch_timestamps"
        expected_column_names = [
            "batch_id",
            "stage",
            "status",
            "timestamp",
            "message_count",
        ]

        # Act
        sut = BatchTimestampsConnector()

        # Assert
        self.assertEqual(expected_table_name, sut._table_name)
        self.assertEqual(expected_column_names, sut._column_names)
        self.assertEqual(mock_clickhouse_batch_sender_instance, sut._batch_sender)

        mock_clickhouse_batch_sender.assert_called_once_with(
            table_name=expected_table_name,
            column_names=expected_column_names,
        )

    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_insert_all_given(self, mock_clickhouse_batch_sender):
        # Arrange
        batch_id = "7299539b-6215-4f6b-b39f-69335aafbeff"
        stage = "prefilter"
        status = "prefilter_out"
        timestamp = "2034-12-13 12:35:35.542635"
        message_count = 456

        sut = BatchTimestampsConnector()

        with patch.object(sut, "_add_to_batch", MagicMock()) as mock_add_to_batch:
            # Act
            sut.insert(
                batch_id=batch_id,
                stage=stage,
                status=status,
                timestamp=timestamp,
                message_count=message_count,
            )

            # Assert
            mock_add_to_batch.assert_called_once_with(
                [
                    uuid.UUID("7299539b-6215-4f6b-b39f-69335aafbeff"),
                    "prefilter",
                    "prefilter_out",
                    datetime.datetime(2034, 12, 13, 12, 35, 35, 542635),
                    456,
                ]
            )

    @patch("src.monitoring.clickhouse_connector.ClickHouseBatchSender")
    def test_insert_none_given(self, mock_clickhouse_batch_sender):
        # Arrange
        batch_id = uuid.UUID("7299539b-6215-4f6b-b39f-69335aafbeff")
        stage = "prefilter"
        status = "prefilter_out"
        message_count = 456

        sut = BatchTimestampsConnector()

        with patch.object(sut, "_add_to_batch", MagicMock()) as mock_add_to_batch:
            # Act
            sut.insert(
                batch_id=batch_id,
                stage=stage,
                status=status,
                message_count=message_count,
            )

            # Assert
            mock_add_to_batch.assert_called_once()


if __name__ == "__main__":
    unittest.main()
