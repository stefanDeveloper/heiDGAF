import unittest
from unittest.mock import patch, Mock

from src.monitoring.clickhouse_batch_sender import ClickHouseBatchSender


class TestInit(unittest.TestCase):
    @patch("src.monitoring.clickhouse_batch_sender.BATCH_SIZE", 50)
    @patch("src.monitoring.clickhouse_batch_sender.BATCH_TIMEOUT", 0.5)
    @patch("src.monitoring.clickhouse_batch_sender.CLICKHOUSE_HOSTNAME", "test_name")
    @patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect")
    def test_init(self, mock_clickhouse_connect):
        # Arrange
        table_name = "test_table_name"
        column_names = ["col_1", "col_2"]

        # Act
        sut = ClickHouseBatchSender(table_name, column_names)

        # Assert
        self.assertEqual(table_name, sut.table_name)
        self.assertEqual(column_names, sut.column_names)
        self.assertEqual(50, sut.max_batch_size)
        self.assertEqual(0.5, sut.batch_timeout)
        self.assertIsNone(sut.timer)
        self.assertEqual([], sut.batch)

        mock_clickhouse_connect.get_client.assert_called_once_with(host="test_name")


class TestDel(unittest.TestCase):
    @patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect")
    def test_del(self, mock_clickhouse_connect):
        # Arrange
        table_name = "test_table_name"
        column_names = ["col_1", "col_2"]
        sut = ClickHouseBatchSender(table_name, column_names)

        # Act
        with patch(
            "src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender.insert_all"
        ) as mock_insert_all:
            del sut

        # Assert
        mock_insert_all.assert_called_once()


class TestAdd(unittest.TestCase):
    @patch("src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender.insert_all")
    @patch("src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender._start_timer")
    @patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect")
    def test_add_list_of_str_successful(
        self, mock_clickhouse_connect, mock_start_timer, mock_insert_all
    ):
        # Arrange
        table_name = "test_table_name"
        column_names = ["col_1", "col_2"]
        sut = ClickHouseBatchSender(table_name, column_names)

        data = ["entry_1", "entry_2"]

        # Act
        sut.add(data)

        # Assert
        self.assertEqual([["entry_1", "entry_2"]], sut.batch)

        mock_insert_all.assert_not_called()
        mock_start_timer.assert_called_once()

    @patch("src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender.insert_all")
    @patch("src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender._start_timer")
    @patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect")
    def test_add_timer_already_started(
        self, mock_clickhouse_connect, mock_start_timer, mock_insert_all
    ):
        # Arrange
        table_name = "test_table_name"
        column_names = ["col_1", "col_2"]
        sut = ClickHouseBatchSender(table_name, column_names)

        data = ["entry_1", "entry_2"]
        sut.timer = Mock()

        # Act
        sut.add(data)

        # Assert
        self.assertEqual([["entry_1", "entry_2"]], sut.batch)

        mock_insert_all.assert_not_called()
        mock_start_timer.assert_not_called()

    @patch("src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender.insert_all")
    @patch("src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender._start_timer")
    @patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect")
    def test_add_max_size_reached_and_timer_already_started(
        self, mock_clickhouse_connect, mock_start_timer, mock_insert_all
    ):
        # Arrange
        table_name = "test_table_name"
        column_names = ["col_1", "col_2"]
        sut = ClickHouseBatchSender(table_name, column_names)

        data = ["entry_1", "entry_2"]
        sut.timer = Mock()
        sut.max_batch_size = 1

        # Act
        sut.add(data)

        # Assert
        self.assertEqual([["entry_1", "entry_2"]], sut.batch)

        mock_insert_all.assert_called_once()
        mock_start_timer.assert_not_called()

    @patch("src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender.insert_all")
    @patch("src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender._start_timer")
    @patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect")
    def test_add_list_of_str_wrong_field_number(
        self, mock_clickhouse_connect, mock_start_timer, mock_insert_all
    ):
        # Arrange
        table_name = "test_table_name"
        column_names = ["col_1"]
        sut = ClickHouseBatchSender(table_name, column_names)

        data = ["entry_1", "entry_2"]

        # Act
        with self.assertRaises(ValueError):
            sut.add(data)

        # Assert
        self.assertEqual([], sut.batch)

        mock_insert_all.assert_not_called()
        mock_start_timer.assert_not_called()

    @patch("src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender.insert_all")
    @patch("src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender._start_timer")
    @patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect")
    def test_add_list_of_lists_successful(
        self, mock_clickhouse_connect, mock_start_timer, mock_insert_all
    ):
        # Arrange
        table_name = "test_table_name"
        column_names = ["col_1", "col_2"]
        sut = ClickHouseBatchSender(table_name, column_names)

        data = [["entry_1", "entry_2"], ["entry_3", "entry_4"]]

        # Act
        sut.add(data)

        # Assert
        self.assertEqual([["entry_1", "entry_2"], ["entry_3", "entry_4"]], sut.batch)

        mock_insert_all.assert_not_called()
        mock_start_timer.assert_called_once()

    @patch("src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender.insert_all")
    @patch("src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender._start_timer")
    @patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect")
    def test_add_list_of_lists_wrong_field_number(
        self, mock_clickhouse_connect, mock_start_timer, mock_insert_all
    ):
        # Arrange
        table_name = "test_table_name"
        column_names = ["col_1"]
        sut = ClickHouseBatchSender(table_name, column_names)

        data = [["entry_1", "entry_2"], ["entry_3"]]

        # Act
        with self.assertRaises(ValueError):
            sut.add(data)

        # Assert
        self.assertEqual([], sut.batch)

        mock_insert_all.assert_not_called()
        mock_start_timer.assert_not_called()

    @patch("src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender.insert_all")
    @patch("src.monitoring.clickhouse_batch_sender.ClickHouseBatchSender._start_timer")
    @patch("src.monitoring.clickhouse_batch_sender.clickhouse_connect")
    def test_add_mixed_types(
        self, mock_clickhouse_connect, mock_start_timer, mock_insert_all
    ):
        # Arrange
        table_name = "test_table_name"
        column_names = ["col_1"]
        sut = ClickHouseBatchSender(table_name, column_names)

        data = [["entry_1"], "entry_2"]

        # Act
        with self.assertRaises(TypeError):
            sut.add(data)

        # Assert
        self.assertEqual([], sut.batch)

        mock_insert_all.assert_not_called()
        mock_start_timer.assert_not_called()


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
