import unittest
from datetime import datetime, timedelta
from unittest.mock import patch, Mock

from benchmarking.src.test_types.base import BaseTest


class TestInit(unittest.TestCase):
    def test_successful_interval_based(self):
        # Arrange
        test_total_message_count = 150
        test_is_interval_based = True

        # Act
        with patch("benchmarking.src.test_types.base.BenchmarkDatasetGenerator"), patch(
            "benchmarking.src.test_types.base.SimpleKafkaProduceHandler"
        ):
            sut = BaseTest(test_total_message_count, test_is_interval_based)

        # Assert
        self.assertIsNone(sut.custom_fields)
        self.assertIsNone(sut.progress_bar)
        self.assertIsNone(sut.start_timestamp)
        self.assertEqual(sut.total_message_count, 150)
        self.assertTrue(sut.is_interval_based)

    def test_successful_not_interval_based(self):
        # Arrange
        test_total_message_count = 150
        test_is_interval_based = False

        # Act
        with patch("benchmarking.src.test_types.base.BenchmarkDatasetGenerator"), patch(
            "benchmarking.src.test_types.base.SimpleKafkaProduceHandler"
        ):
            sut = BaseTest(test_total_message_count, test_is_interval_based)

        # Assert
        self.assertIsNone(sut.custom_fields)
        self.assertIsNone(sut.progress_bar)
        self.assertIsNone(sut.start_timestamp)
        self.assertEqual(sut.total_message_count, 150)
        self.assertFalse(sut.is_interval_based)

    def test_failed_too_low_message_count(self):
        # Arrange
        test_total_message_count = 0
        test_is_interval_based = False

        # Act and Assert
        with self.assertRaises(ValueError):
            with patch(
                "benchmarking.src.test_types.base.BenchmarkDatasetGenerator"
            ), patch("benchmarking.src.test_types.base.SimpleKafkaProduceHandler"):
                BaseTest(test_total_message_count, test_is_interval_based)

    def test_failed_negative_message_count(self):
        # Arrange
        test_total_message_count = -10
        test_is_interval_based = True

        # Act and Assert
        with self.assertRaises(ValueError):
            with patch(
                "benchmarking.src.test_types.base.BenchmarkDatasetGenerator"
            ), patch("benchmarking.src.test_types.base.SimpleKafkaProduceHandler"):
                BaseTest(test_total_message_count, test_is_interval_based)


class TestExecute(unittest.TestCase):
    def setUp(self):
        """Mocks the logger to deactivate logs in test run."""
        patcher = patch("benchmarking.src.test_types.base.logger")
        self.mock_logger = patcher.start()
        self.addCleanup(patcher.stop)

    def test_successful(self):
        # Arrange
        with patch("benchmarking.src.test_types.base.BenchmarkDatasetGenerator"), patch(
            "benchmarking.src.test_types.base.SimpleKafkaProduceHandler"
        ):
            sut = BaseTest(120, True)

        self.assertIsNone(sut.custom_fields)
        self.assertIsNone(sut.progress_bar)
        self.assertIsNone(sut.start_timestamp)

        mock_progress_bar_instance = Mock()

        with patch(
            "benchmarking.src.test_types.base.BaseTest._setup_progress_bar"
        ) as mock_setup_progress_bar_method, patch(
            "benchmarking.src.test_types.base.BaseTest._execute_core"
        ) as mock_execute_core_method:
            mock_setup_progress_bar_method.return_value = (
                mock_progress_bar_instance,
                "test",
            )

            # Act
            sut.execute()

        # Assert
        mock_setup_progress_bar_method.assert_called_once()

        mock_progress_bar_instance.start.assert_called_once()
        mock_execute_core_method.assert_called_once()
        mock_progress_bar_instance.finish.assert_called_once()

        self.assertIsNone(sut.custom_fields)
        self.assertIsNone(sut.progress_bar)
        self.assertIsNone(sut.start_timestamp)


class TestExecuteCore(unittest.TestCase):
    def test_not_implemented(self):
        # Arrange
        with patch("benchmarking.src.test_types.base.BenchmarkDatasetGenerator"), patch(
            "benchmarking.src.test_types.base.SimpleKafkaProduceHandler"
        ):
            sut = BaseTest(120, True)

        # Act and Assert
        with self.assertRaises(NotImplementedError):
            sut._execute_core()


class TestSetupProgressBar(unittest.TestCase):
    def test_successful_with_intervals(self):
        # Arrange
        test_is_interval_based = True

        with patch("benchmarking.src.test_types.base.BenchmarkDatasetGenerator"), patch(
            "benchmarking.src.test_types.base.SimpleKafkaProduceHandler"
        ):
            sut = BaseTest(120, test_is_interval_based)

        # Act
        returned_progress_bar, returned_custom_fields = sut._setup_progress_bar()

        # Assert
        self.assertIsNotNone(returned_progress_bar)
        self.assertIsNotNone(returned_custom_fields.get("message_count"))
        self.assertNotEquals("", returned_custom_fields.get("interval").format)

    def test_successful_without_intervals(self):
        # Arrange
        test_is_interval_based = False

        with patch("benchmarking.src.test_types.base.BenchmarkDatasetGenerator"), patch(
            "benchmarking.src.test_types.base.SimpleKafkaProduceHandler"
        ):
            sut = BaseTest(120, test_is_interval_based)

        # Act
        returned_progress_bar, returned_custom_fields = sut._setup_progress_bar()

        # Assert
        self.assertIsNotNone(returned_progress_bar)
        self.assertIsNotNone(returned_custom_fields.get("message_count"))
        self.assertEquals("", returned_custom_fields.get("interval").format)


class TestGetTimeElapsed(unittest.TestCase):
    def test_successful(self):
        # Arrange
        with patch("benchmarking.src.test_types.base.BenchmarkDatasetGenerator"), patch(
            "benchmarking.src.test_types.base.SimpleKafkaProduceHandler"
        ):
            sut = BaseTest(120, True)

        sut.progress_bar = Mock()
        sut.progress_bar.start_time = datetime.now()

        # Act
        returned_time_elapsed = sut._get_time_elapsed()

        # Assert
        self.assertGreaterEqual(returned_time_elapsed, timedelta(0))


if __name__ == "__main__":
    unittest.main()
