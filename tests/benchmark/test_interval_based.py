import unittest
from unittest.mock import patch, Mock, call

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
    def test_one_interval(self):
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
        expected_update_mapping_calls = (
            call(interval_number=1),
            call(interval_number=2),
            call(interval_number=3),
        )
        sut.custom_fields["interval"].update_mapping.assert_has_calls(
            expected_update_mapping_calls
        )

        expected_execute_single_interval_calls = (
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
        )
        mock_execute_single_interval.assert_has_calls(
            expected_execute_single_interval_calls
        )


if __name__ == "__main__":
    unittest.main()
