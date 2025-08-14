import datetime
import unittest
from unittest.mock import patch, Mock, call, ANY

from confluent_kafka import KafkaException

from benchmarking.src.test_types.base import IntervalBasedTest


class TestInit(unittest.TestCase):
    def test_successful(self):
        # Arrange
        test_interval_lengths_in_seconds = [1, 2, 3]
        test_messages_per_second_in_intervals = [4, 5, 6]

        with patch(
            "benchmarking.src.test_types.base.IntervalBasedTest._IntervalBasedTest__handle_single_interval_value"
        ) as mock_handle_single_interval_value, patch(
            "benchmarking.src.test_types.base.IntervalBasedTest._IntervalBasedTest__validate_interval_data"
        ) as mock_validate_interval_data, patch(
            "benchmarking.src.test_types.base.IntervalBasedTest._IntervalBasedTest__get_total_message_count"
        ) as mock_get_total_message_count, patch(
            "benchmarking.src.test_types.base.BaseTest.__init__"
        ) as mock_base_test_init:
            mock_get_total_message_count.return_value = 150

            # Act
            sut = IntervalBasedTest(
                test_interval_lengths_in_seconds, test_messages_per_second_in_intervals
            )

        # Assert
        self.assertEqual(sut.interval_lengths_in_seconds, [1, 2, 3])
        self.assertEqual(sut.messages_per_second_in_intervals, [4, 5, 6])

        mock_handle_single_interval_value.assert_called_once()
        mock_validate_interval_data.assert_called_once()

        mock_base_test_init.assert_called_once_with(
            is_interval_based=True,
            total_message_count=150,
        )


class TestExecuteCore(unittest.TestCase):
    def test_single_interval(self):
        # Arrange
        test_interval_length = 4
        test_messages_per_second_in_intervals = [150]

        with patch("benchmarking.src.test_types.base.BaseTest.__init__"):
            sut = IntervalBasedTest(
                interval_lengths_in_seconds=test_interval_length,
                messages_per_second_in_intervals=test_messages_per_second_in_intervals,
            )
            sut.custom_fields = {"interval": Mock()}

        with patch(
            "benchmarking.src.test_types.base.IntervalBasedTest._IntervalBasedTest__execute_single_interval"
        ) as mock_execute_single_interval:
            # Act
            sut._execute_core()

        # Assert
        sut.custom_fields["interval"].update_mapping.assert_called_once_with(
            interval_number=1
        )
        mock_execute_single_interval.assert_called_once_with(
            current_index=0,
            messages_per_second=150,
            length_in_seconds=4,
        )

    def test_multiple_intervals(self):
        # Arrange
        test_interval_length = [3, 2, 1]
        test_messages_per_second_in_intervals = [170, 100, 50]

        with patch("benchmarking.src.test_types.base.BaseTest.__init__"):
            sut = IntervalBasedTest(
                interval_lengths_in_seconds=test_interval_length,
                messages_per_second_in_intervals=test_messages_per_second_in_intervals,
            )
            sut.custom_fields = {"interval": Mock()}

        with patch(
            "benchmarking.src.test_types.base.IntervalBasedTest._IntervalBasedTest__execute_single_interval"
        ) as mock_execute_single_interval:
            mock_execute_single_interval.side_effect = [1, 2, 3]

            # Act
            sut._execute_core()

        # Assert
        expected_update_mapping_calls = [
            call(interval_number=1),
            call(interval_number=2),
            call(interval_number=3),
        ]
        self.assertEqual(
            len(sut.custom_fields["interval"].update_mapping.mock_calls), 3
        )
        sut.custom_fields["interval"].update_mapping.assert_has_calls(
            expected_update_mapping_calls
        )

        expected_execute_single_interval_calls = [
            call(
                current_index=0,
                messages_per_second=170,
                length_in_seconds=3,
            ),
            call(
                current_index=1,
                messages_per_second=100,
                length_in_seconds=2,
            ),
            call(
                current_index=2,
                messages_per_second=50,
                length_in_seconds=1,
            ),
        ]
        self.assertEqual(len(mock_execute_single_interval.mock_calls), 3)
        mock_execute_single_interval.assert_has_calls(
            expected_execute_single_interval_calls
        )


class TestExecuteSingleInterval(unittest.TestCase):
    def setUp(self):
        """Mocks the logger to deactivate logs in test run."""
        patcher = patch("benchmarking.src.test_types.base.logger")
        self.mock_logger = patcher.start()
        self.addCleanup(patcher.stop)

    @patch("benchmarking.src.test_types.base.PRODUCE_TO_TOPIC", "test_topic")
    def test_successful(self):
        # Arrange
        test_interval_length = [3, 5, 1, 3, 2, 1]
        test_messages_per_second_in_intervals = [170, 100, 50, 15, 156, 135]

        with patch("benchmarking.src.test_types.base.BaseTest.__init__"):
            sut = IntervalBasedTest(
                interval_lengths_in_seconds=test_interval_length,
                messages_per_second_in_intervals=test_messages_per_second_in_intervals,
            )
            sut.kafka_producer = Mock()
            sut.dataset_generator = Mock()
            sut.custom_fields = {"message_count": Mock()}
            sut.progress_bar = Mock()

        with patch("benchmarking.src.test_types.base.datetime") as mock_datetime, patch(
            "benchmarking.src.test_types.base.time.sleep"
        ) as mock_time_sleep, patch(
            "benchmarking.src.test_types.base.IntervalBasedTest._get_time_elapsed"
        ) as mock_get_time_elapsed, patch(
            "benchmarking.src.test_types.base.IntervalBasedTest._IntervalBasedTest__get_total_duration"
        ) as mock_get_total_duration:
            mock_datetime.now.side_effect = [
                datetime.datetime(2025, 1, 1, 12, 0, 0, 0),
                datetime.datetime(2025, 1, 1, 12, 0, 1, 0),
                datetime.datetime(2025, 1, 1, 12, 0, 4, 0),
                datetime.datetime(2025, 1, 1, 12, 0, 6, 0),
            ]
            mock_get_time_elapsed.side_effect = [
                datetime.timedelta(3),
                datetime.timedelta(4.5),
            ]
            mock_get_total_duration.return_value = datetime.timedelta(15)

            # Act
            returned_value = sut._IntervalBasedTest__execute_single_interval(  # noqa
                current_index=1,
                messages_per_second=100,
                length_in_seconds=5,
            )

        # Assert
        self.assertEqual(len(sut.kafka_producer.produce.mock_calls), 2)
        sut.kafka_producer.produce.assert_called_with("test_topic", ANY)

        expected_update_mapping_calls = [
            call(current_message_count=1),
            call(current_message_count=2),
        ]
        self.assertEqual(
            len(sut.custom_fields["message_count"].update_mapping.mock_calls), 2
        )
        sut.custom_fields["message_count"].update_mapping.assert_has_calls(
            expected_update_mapping_calls
        )

        expected_progress_bar_update_calls = (call(20.0), call(30.0))
        self.assertEqual(len(sut.progress_bar.update.mock_calls), 2)
        sut.progress_bar.update.assert_has_calls(expected_progress_bar_update_calls)

        self.assertEqual(len(mock_time_sleep.mock_calls), 2)
        mock_time_sleep.assert_called_with(0.01)

        self.assertEqual(returned_value, 3)

    @patch("benchmarking.src.test_types.base.PRODUCE_TO_TOPIC", "test_topic")
    def test_including_kafka_error(self):
        # Arrange
        test_interval_length = [3, 5, 1, 3, 2, 1]
        test_messages_per_second_in_intervals = [170, 100, 50, 15, 156, 135]

        with patch("benchmarking.src.test_types.base.BaseTest.__init__"):
            sut = IntervalBasedTest(
                interval_lengths_in_seconds=test_interval_length,
                messages_per_second_in_intervals=test_messages_per_second_in_intervals,
            )
            sut.kafka_producer = Mock()
            sut.dataset_generator = Mock()
            sut.custom_fields = {"message_count": Mock()}
            sut.progress_bar = Mock()

            sut.kafka_producer.produce.side_effect = [KafkaException, None]

        with patch("benchmarking.src.test_types.base.datetime") as mock_datetime, patch(
            "benchmarking.src.test_types.base.time.sleep"
        ) as mock_time_sleep, patch(
            "benchmarking.src.test_types.base.IntervalBasedTest._get_time_elapsed"
        ) as mock_get_time_elapsed, patch(
            "benchmarking.src.test_types.base.IntervalBasedTest._IntervalBasedTest__get_total_duration"
        ) as mock_get_total_duration:
            mock_datetime.now.side_effect = [
                datetime.datetime(2025, 1, 1, 12, 0, 0, 0),
                datetime.datetime(2025, 1, 1, 12, 0, 1, 0),
                datetime.datetime(2025, 1, 1, 12, 0, 4, 0),
                datetime.datetime(2025, 1, 1, 12, 0, 6, 0),
            ]
            mock_get_time_elapsed.side_effect = [datetime.timedelta(4.5)]
            mock_get_total_duration.return_value = datetime.timedelta(15)

            # Act
            returned_value = sut._IntervalBasedTest__execute_single_interval(  # noqa
                current_index=1,
                messages_per_second=100,
                length_in_seconds=5,
            )

        # Assert
        self.assertEqual(len(sut.kafka_producer.produce.mock_calls), 2)
        sut.kafka_producer.produce.assert_called_with("test_topic", ANY)

        sut.custom_fields["message_count"].update_mapping.assert_called_once_with(
            current_message_count=1
        )

        sut.progress_bar.update.assert_called_once_with(30.0)

        self.assertEqual(len(mock_time_sleep.mock_calls), 2)
        mock_time_sleep.assert_called_with(0.01)

        self.assertEqual(returned_value, 2)


class TestGetTotalDuration(unittest.TestCase):
    def test_single_interval(self):
        # Arrange
        test_interval_length = 47
        test_messages_per_second_in_intervals = [170]

        with patch("benchmarking.src.test_types.base.BaseTest.__init__"):
            sut = IntervalBasedTest(
                interval_lengths_in_seconds=test_interval_length,
                messages_per_second_in_intervals=test_messages_per_second_in_intervals,
            )

        # Act
        returned_value = sut._IntervalBasedTest__get_total_duration()  # noqa

        # Assert
        self.assertEqual(returned_value, datetime.timedelta(seconds=47))

    def test_multiple_intervals(self):
        # Arrange
        test_interval_length = [1, 2, 3, 4, 5, 6]  # sum = 21
        test_messages_per_second_in_intervals = [170, 100, 50, 15, 156, 135]

        with patch("benchmarking.src.test_types.base.BaseTest.__init__"):
            sut = IntervalBasedTest(
                interval_lengths_in_seconds=test_interval_length,
                messages_per_second_in_intervals=test_messages_per_second_in_intervals,
            )

        # Act
        returned_value = sut._IntervalBasedTest__get_total_duration()  # noqa

        # Assert
        self.assertEqual(returned_value, datetime.timedelta(seconds=21))


if __name__ == "__main__":
    unittest.main()
