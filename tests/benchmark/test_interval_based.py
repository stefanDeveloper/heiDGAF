import unittest
from unittest.mock import patch

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
        sut.interval_lengths_in_seconds = [1, 2, 3]
        sut.messages_per_second_in_intervals = [4, 5, 6]

        mock_handle_single_interval_value.assert_called_once()
        mock_validate_interval_data.assert_called_once()

        mock_base_test_init.assert_called_once_with(
            is_interval_based=True,
            total_message_count=150,
        )
