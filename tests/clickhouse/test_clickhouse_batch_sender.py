import datetime
import unittest
from typing import Optional
from unittest.mock import patch, Mock

from src.monitoring.clickhouse_batch_sender import ClickHouseBatchSender, Table


class TestTable(unittest.TestCase):
    def test_verify_with_wrong_number_of_fields(self):
        # Arrange
        sut = Table(name="test_table", columns={"col1": str, "col2": int})

        # Act and Assert
        with self.assertRaises(ValueError):
            sut.verify({"col1": "value"})

    def test_verify_with_unexpected_field(self):
        # Arrange
        sut = Table(name="test_table", columns={"col1": str, "col2": int})

        # Act and Assert
        with self.assertRaises(ValueError):
            sut.verify({"col1": "value", "unexpected": 32})

    def test_verify_successful(self):
        # Arrange
        sut = Table(name="test_table", columns={"col1": str, "col2": int})

        # Act and Assert
        sut.verify({"col1": "value", "col2": 32})

    def test_verify_with_wrong_type(self):
        # Arrange
        sut = Table(name="test_table", columns={"col1": str, "col2": int})

        # Act and Assert
        with self.assertRaises(TypeError):
            sut.verify({"col1": 32, "col2": 32})

    def test_verify_with_optional_value(self):
        # Arrange
        sut = Table(name="test_table", columns={"col1": str, "col2": Optional[int]})

        # Act and Assert
        sut.verify({"col1": "value", "col2": 32})
        sut.verify({"col1": "value", "col2": None})

        with self.assertRaises(ValueError):
            sut.verify({"col1": "value"})


class TestInit(unittest.TestCase):
    @patch("src.monitoring.clickhouse_batch_sender.BATCH_SIZE", 50)
    @patch("src.monitoring.clickhouse_batch_sender.BATCH_TIMEOUT", 0.5)
    @patch("src.monitoring.clickhouse_batch_sender.CLICKHOUSE_HOSTNAME", "test_name")
    @patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect")
    def test_init(self, mock_clickhouse_connect):
        # Act
        sut = ClickHouseBatchSender()

        # Assert
        self.assertEqual(50, sut.max_batch_size)
        self.assertEqual(0.5, sut.batch_timeout)
        self.assertIsNone(sut.timer)
        self.assertEqual(dict, type(sut.batch))

        mock_clickhouse_connect.get_client.assert_called_once_with(host="test_name")


class TestDel(unittest.TestCase):
    @patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect")
    def test_del(self, mock_clickhouse_connect):
        # Arrange
        table_name = "test_table_name"
        column_names = ["col_1", "col_2"]
        sut = ClickHouseBatchSender()

        # Act
        with patch(
            "src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender.insert_all"
        ) as mock_insert_all:
            del sut

        # Assert
        mock_insert_all.assert_called_once()


class TestAdd(unittest.TestCase):
    @patch("src.monitoring.clickhouse_batch_sender.Table")
    @patch("src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender.insert")
    @patch("src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender._start_timer")
    @patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect")
    def test_add_list_of_str_successful(
        self, mock_clickhouse_connect, mock_start_timer, mock_insert, mock_table
    ):
        # Arrange
        table_name = "fill_levels"
        sut = ClickHouseBatchSender()

        now = datetime.datetime.now()
        data = {
            "timestamp": now,
            "stage": "test_stage",
            "entry_type": "test_type",
            "entry_count": 23,
        }

        # Act
        sut.add(table_name, data)

        # Assert
        self.assertEqual(
            [[now, "test_stage", "test_type", 23]], sut.batch.get(table_name)
        )

        mock_insert.assert_not_called()
        mock_start_timer.assert_called_once()

    @patch("src.monitoring.clickhouse_batch_sender.Table")
    @patch("src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender.insert")
    @patch("src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender._start_timer")
    @patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect")
    def test_add_timer_already_started(
        self, mock_clickhouse_connect, mock_start_timer, mock_insert, mock_table
    ):
        # Arrange
        table_name = "fill_levels"
        sut = ClickHouseBatchSender()

        now = datetime.datetime.now()
        data = {
            "timestamp": now,
            "stage": "test_stage",
            "entry_type": "test_type",
            "entry_count": 23,
        }
        sut.timer = Mock()

        # Act
        sut.add(table_name, data)

        # Assert
        self.assertEqual(
            [[now, "test_stage", "test_type", 23]], sut.batch.get(table_name)
        )

        mock_insert.assert_not_called()
        mock_start_timer.assert_not_called()

    @patch("src.monitoring.clickhouse_batch_sender.Table")
    @patch("src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender.insert")
    @patch("src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender._start_timer")
    @patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect")
    def test_add_max_size_reached_and_timer_already_started(
        self, mock_clickhouse_connect, mock_start_timer, mock_insert, mock_table
    ):
        # Arrange
        table_name = "fill_levels"
        sut = ClickHouseBatchSender()

        now = datetime.datetime.now()
        data = {
            "timestamp": now,
            "stage": "test_stage",
            "entry_type": "test_type",
            "entry_count": 23,
        }
        sut.timer = Mock()
        sut.max_batch_size = 1

        # Act
        sut.add(table_name, data)

        # Assert
        self.assertEqual(
            [[now, "test_stage", "test_type", 23]], sut.batch.get(table_name)
        )

        mock_insert.assert_called_once()
        mock_start_timer.assert_not_called()


class TestInsertAll(unittest.TestCase):
    @patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect")
    def test_insert_all(self, mock_clickhouse_connect):
        # Arrange
        table_name = "fill_levels"
        sut = ClickHouseBatchSender()
        sut._client = Mock()
        first = datetime.datetime.now()
        second = datetime.datetime.now()
        sut.batch[table_name] = [
            [first, "test_stage", "test_type", 23],
            [second, "test_stage", "test_type", 24],
        ]

        # Act
        sut.insert_all()

        # Assert
        self.assertEqual([], sut.batch.get(table_name))
        self.assertIsNone(sut.timer)

        sut._client.insert.assert_called_once_with(
            table_name,
            [
                [first, "test_stage", "test_type", 23],
                [second, "test_stage", "test_type", 24],
            ],
            column_names=["timestamp", "stage", "entry_type", "entry_count"],
        )

    @patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect")
    def test_insert_all_with_timer(self, mock_clickhouse_connect):
        # Arrange
        table_name = "fill_levels"
        sut = ClickHouseBatchSender()
        sut._client = Mock()
        sut.timer = Mock()
        first = datetime.datetime.now()
        second = datetime.datetime.now()
        sut.batch[table_name] = [
            [first, "test_stage", "test_type", 23],
            [second, "test_stage", "test_type", 24],
        ]

        # Act
        sut.insert_all()

        # Assert
        self.assertEqual([], sut.batch.get(table_name))
        self.assertIsNone(sut.timer)

        sut._client.insert.assert_called_once_with(
            table_name,
            [
                [first, "test_stage", "test_type", 23],
                [second, "test_stage", "test_type", 24],
            ],
            column_names=["timestamp", "stage", "entry_type", "entry_count"],
        )


class TestStartTimer(unittest.TestCase):
    @patch("src.monitoring.clickhouse_batch_sender.BATCH_TIMEOUT", 0.5)
    @patch("src.monitoring.clickhouse_batch_sender.Timer")
    @patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect")
    def test_start_timer(self, mock_clickhouse_connect, mock_timer):
        # Arrange
        sut = ClickHouseBatchSender()

        # Act
        sut._start_timer()

        # Assert
        mock_timer.assert_called_once_with(
            0.5,
            sut.insert_all,
        )
        mock_timer.cancel.assert_not_called()
        sut.timer.start.assert_called_once()

    @patch("src.monitoring.clickhouse_batch_sender.BATCH_TIMEOUT", 0.5)
    @patch("src.monitoring.clickhouse_batch_sender.Timer")
    @patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect")
    def test_start_timer_with_running_timer(self, mock_clickhouse_connect, mock_timer):
        # Arrange
        sut = ClickHouseBatchSender()
        sut.timer = mock_timer

        # Act
        sut._start_timer()

        # Assert
        mock_timer.assert_called_once_with(
            0.5,
            sut.insert_all,
        )
        mock_timer.cancel.assert_called_once()
        sut.timer.start.assert_called_once()
