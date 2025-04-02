import unittest
from unittest.mock import patch, Mock

from src.monitoring.clickhouse_batch_sender import ClickHouseBatchSender, Table


class TestInit(unittest.TestCase):

    def test_successful(self):
        # Act
        with (
            patch("src.monitoring.clickhouse_batch_sender.BATCH_SIZE", 50),
            patch("src.monitoring.clickhouse_batch_sender.BATCH_TIMEOUT", 0.5),
            patch(
                "src.monitoring.clickhouse_batch_sender.CLICKHOUSE_HOSTNAME",
                "test_name",
            ),
            patch(
                "src.monitoring.clickhouse_batch_sender.clickhouse_connect"
            ) as mock_clickhouse_connect,
        ):
            sut = ClickHouseBatchSender()

        # Assert
        self.assertIsNotNone(sut.tables)
        self.assertEqual(50, sut.max_batch_size)
        self.assertEqual(0.5, sut.batch_timeout)
        self.assertIsNone(sut.timer)
        self.assertIsNotNone(sut.lock)
        self.assertEqual({key: [] for key in sut.tables}, sut.batch)

        mock_clickhouse_connect.get_client.assert_called_once_with(host="test_name")


class TestDel(unittest.TestCase):

    def setUp(self):
        with patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect"):
            self.sut = ClickHouseBatchSender()

    def test_del(self):
        # Act
        with patch(
            "src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender.insert_all"
        ) as mock_insert_all:
            del self.sut

        # Assert
        mock_insert_all.assert_called_once()


class TestAdd(unittest.TestCase):

    def setUp(self):
        with patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect"):
            self.sut = ClickHouseBatchSender()

    def test_single_list_with_starting_timer(self):
        # Arrange
        test_table_name = "test_table"
        test_data = {"value_1": 1, "value_2": 2}

        self.sut.tables = {test_table_name: Table(test_table_name, {})}
        self.sut.batch = {test_table_name: []}

        # Act
        with (
            patch("src.monitoring.clickhouse_batch_sender.Table.verify"),
            patch(
                "src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender._start_timer"
            ) as mock_start_timer,
        ):
            self.sut.add(test_table_name, test_data)

        # Assert
        self.sut.batch = {test_table_name: [1, 2]}
        mock_start_timer.assert_called_once()

    def test_timer_already_started(self):
        # Arrange
        test_table_name = "test_table"
        test_data = {"value_1": 1, "value_2": 2}

        self.sut.tables = {test_table_name: Table(test_table_name, {})}
        self.sut.batch = {test_table_name: []}
        self.sut.timer = Mock()

        # Act
        with (
            patch("src.monitoring.clickhouse_batch_sender.Table.verify"),
            patch(
                "src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender._start_timer"
            ) as mock_start_timer,
        ):
            self.sut.add(test_table_name, test_data)

        # Assert
        mock_start_timer.assert_not_called()

    def test_max_batch_size_reached(self):
        # Arrange
        test_table_name = "test_table"
        test_data = {"value_1": 1, "value_2": 2}

        self.sut.tables = {test_table_name: Table(test_table_name, {})}
        self.sut.batch = {test_table_name: []}
        self.sut.max_batch_size = 1

        # Act
        with (
            patch("src.monitoring.clickhouse_batch_sender.Table.verify"),
            patch(
                "src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender.insert"
            ) as mock_insert,
            patch(
                "src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender._start_timer"
            ),
        ):
            self.sut.add(test_table_name, test_data)

        # Assert
        mock_insert.assert_called_once_with(test_table_name)


class TestInsertAll(unittest.TestCase):
    @patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect")
    def test_insert_all(self, mock_clickhouse_connect):
        # Arrange
        table_name = "test_table_name"
        column_names = ["col_1", "col_2"]
        sut = ClickHouseBatchSender(table_name, column_names)
        sut._client = Mock()
        sut.batch = [["entry_1", "entry_2"], ["entry_3", "entry_4"]]

        # Act
        sut.insert_all()

        # Assert
        self.assertEqual([], sut.batch)
        self.assertIsNone(sut.timer)

        sut._client.insert.assert_called_once_with(
            table_name,
            [["entry_1", "entry_2"], ["entry_3", "entry_4"]],
            column_names=column_names,
        )

    @patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect")
    def test_insert_all_with_timer(self, mock_clickhouse_connect):
        # Arrange
        table_name = "test_table_name"
        column_names = ["col_1", "col_2"]
        sut = ClickHouseBatchSender(table_name, column_names)
        sut._client = Mock()
        sut.timer = Mock()
        sut.batch = [["entry_1", "entry_2"]]

        # Act
        sut.insert_all()

        # Assert
        self.assertEqual([], sut.batch)
        self.assertIsNone(sut.timer)

        sut._client.insert.assert_called_once_with(
            table_name,
            [["entry_1", "entry_2"]],
            column_names=column_names,
        )


class TestStartTimer(unittest.TestCase):
    @patch("src.monitoring.clickhouse_batch_sender.BATCH_TIMEOUT", 0.5)
    @patch("src.monitoring.clickhouse_batch_sender.Timer")
    @patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect")
    def test_start_timer(self, mock_clickhouse_connect, mock_timer):
        # Arrange
        table_name = "test_table_name"
        column_names = ["col_1", "col_2"]
        sut = ClickHouseBatchSender(table_name, column_names)

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
        table_name = "test_table_name"
        column_names = ["col_1", "col_2"]
        sut = ClickHouseBatchSender(table_name, column_names)
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
