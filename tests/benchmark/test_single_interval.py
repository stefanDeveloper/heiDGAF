import datetime
import unittest
from unittest.mock import patch, Mock, call, ANY

from confluent_kafka import KafkaException

from benchmarking.src.base_test_types import SingleIntervalTest


class TestInit(unittest.TestCase):
    def test_successful(self):
        # Arrange
        test_full_length_in_minutes = 17.3
        test_messages_per_second = 70

        with patch(
            "benchmarking.src.base_test_types.SingleIntervalTest._SingleIntervalTest__get_total_message_count"
        ) as mock_get_total_message_count, patch(
            "benchmarking.src.base_test_types.BaseTest.__init__"
        ) as mock_base_test_init:
            mock_get_total_message_count.return_value = 150

            # Act
            sut = SingleIntervalTest(
                "SuT",
                test_full_length_in_minutes,
                test_messages_per_second,
            )

        # Assert
        self.assertEqual(sut.full_length_in_minutes, 17.3)
        self.assertEqual(sut.messages_per_second, 70)

        mock_base_test_init.assert_called_once_with(
            name="SuT",
            is_interval_based=False,
            total_message_count=150,
        )


class TestExecuteCore(unittest.TestCase):
    def setUp(self):
        """Mocks the logger to deactivate logs in test run."""
        patcher = patch("benchmarking.src.base_test_types.logger")
        self.mock_logger = patcher.start()
        self.addCleanup(patcher.stop)

    @patch("benchmarking.src.base_test_types.PRODUCE_TO_TOPIC", "test_topic")
    def test_successful(self):
        # Arrange
        test_full_length_in_minutes = 5 / 60  # 5 seconds
        test_messages_per_second = 100

        with patch("benchmarking.src.base_test_types.BaseTest.__init__"):
            sut = SingleIntervalTest(
                name="SuT",
                full_length_in_minutes=test_full_length_in_minutes,
                messages_per_second=test_messages_per_second,
            )
            sut.kafka_producer = Mock()
            sut.dataset_generator = Mock()
            sut.custom_fields = {"message_count": Mock()}
            sut.progress_bar = Mock()

        with patch("benchmarking.src.base_test_types.datetime") as mock_datetime, patch(
            "benchmarking.src.base_test_types.time.sleep"
        ) as mock_time_sleep, patch(
            "benchmarking.src.base_test_types.SingleIntervalTest._get_time_elapsed"
        ) as mock_get_time_elapsed:
            mock_datetime.now.side_effect = [
                datetime.datetime(2025, 1, 1, 12, 0, 0, 0),
                datetime.datetime(2025, 1, 1, 12, 0, 1, 0),
                datetime.datetime(2025, 1, 1, 12, 0, 4, 0),
                datetime.datetime(2025, 1, 1, 12, 0, 6, 0),
            ]
            mock_get_time_elapsed.side_effect = [
                datetime.timedelta(seconds=3),
                datetime.timedelta(seconds=4.5),
            ]

            # Act
            sut._execute_core()

        # Assert
        self.assertEqual(len(sut.kafka_producer.produce.mock_calls), 2)
        sut.kafka_producer.produce.assert_called_with("test_topic", ANY)

        expected_update_mapping_calls = [
            call(current_message_count=0),
            call(current_message_count=1),
        ]
        self.assertEqual(
            len(sut.custom_fields["message_count"].update_mapping.mock_calls), 2
        )
        sut.custom_fields["message_count"].update_mapping.assert_has_calls(
            expected_update_mapping_calls
        )

        expected_progress_bar_update_calls = (call(60.0), call(90.0))
        self.assertEqual(
            len(sut.progress_bar.update.mock_calls), 3
        )  # two inside loop, one outside (to 100)
        sut.progress_bar.update.assert_has_calls(expected_progress_bar_update_calls)

        self.assertEqual(len(mock_time_sleep.mock_calls), 2)
        mock_time_sleep.assert_called_with(0.01)

    @patch("benchmarking.src.base_test_types.PRODUCE_TO_TOPIC", "test_topic")
    def test_including_kafka_error(self):
        # Arrange
        test_full_length_in_minutes = 5 / 60  # 5 seconds
        test_messages_per_second = 100

        with patch("benchmarking.src.base_test_types.BaseTest.__init__"):
            sut = SingleIntervalTest(
                name="SuT",
                full_length_in_minutes=test_full_length_in_minutes,
                messages_per_second=test_messages_per_second,
            )
            sut.kafka_producer = Mock()
            sut.dataset_generator = Mock()
            sut.custom_fields = {"message_count": Mock()}
            sut.progress_bar = Mock()

            sut.kafka_producer.produce.side_effect = [KafkaException, None]

        with patch("benchmarking.src.base_test_types.datetime") as mock_datetime, patch(
            "benchmarking.src.base_test_types.time.sleep"
        ) as mock_time_sleep, patch(
            "benchmarking.src.base_test_types.SingleIntervalTest._get_time_elapsed"
        ) as mock_get_time_elapsed:
            mock_datetime.now.side_effect = [
                datetime.datetime(2025, 1, 1, 12, 0, 0, 0),
                datetime.datetime(2025, 1, 1, 12, 0, 1, 0),
                datetime.datetime(2025, 1, 1, 12, 0, 4, 0),
                datetime.datetime(2025, 1, 1, 12, 0, 6, 0),
            ]
            mock_get_time_elapsed.side_effect = [datetime.timedelta(seconds=4.5)]

            # Act
            sut._execute_core()

        # Assert
        self.assertEqual(len(sut.kafka_producer.produce.mock_calls), 2)
        sut.kafka_producer.produce.assert_called_with("test_topic", ANY)

        sut.custom_fields["message_count"].update_mapping.assert_called_once_with(
            current_message_count=0
        )

        expected_progress_bar_update_calls = (call(90.0), call(100.0))
        self.assertEqual(len(sut.progress_bar.update.mock_calls), 2)
        sut.progress_bar.update.assert_has_calls(expected_progress_bar_update_calls)

        self.assertEqual(len(mock_time_sleep.mock_calls), 2)
        mock_time_sleep.assert_called_with(0.01)


class TestGetTotalMessageCount(unittest.TestCase):
    def test_without_rounding(self):
        test_full_length_in_minutes = 7
        test_messages_per_second = 100

        # Arrange
        with patch("benchmarking.src.base_test_types.BaseTest.__init__"):
            sut = SingleIntervalTest(
                name="SuT",
                full_length_in_minutes=test_full_length_in_minutes,
                messages_per_second=test_messages_per_second,
            )
            sut.full_length_in_minutes = test_full_length_in_minutes
            sut.messages_per_second = test_messages_per_second

        # Act
        returned_value = sut._SingleIntervalTest__get_total_message_count()  # noqa

        # Assert
        self.assertEqual(returned_value, 42000)

    def test_with_rounding(self):
        test_full_length_in_minutes = 7.4
        test_messages_per_second = 92.9

        # Arrange
        with patch("benchmarking.src.base_test_types.BaseTest.__init__"):
            sut = SingleIntervalTest(
                name="SuT",
                full_length_in_minutes=test_full_length_in_minutes,
                messages_per_second=test_messages_per_second,
            )
            sut.full_length_in_minutes = test_full_length_in_minutes
            sut.messages_per_second = test_messages_per_second

        # Act
        returned_value = sut._SingleIntervalTest__get_total_message_count()  # noqa

        # Assert
        self.assertEqual(returned_value, 41248)  # without rounding: 41247.6


if __name__ == "__main__":
    unittest.main()
