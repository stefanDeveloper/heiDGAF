import unittest
from unittest.mock import patch

from benchmarking.src.test_types.base import BaseTest


class TestInit(unittest.TestCase):
    def test_successful_interval_based(self):
        # Arrange
        test_total_message_count = 150
        test_is_interval_based = True

        # Act
        with patch("benchmarking.src.test_types.base.DatasetGenerator"), patch(
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
        with patch("benchmarking.src.test_types.base.DatasetGenerator"), patch(
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

        # Act
        with self.assertRaises(ValueError):
            with patch("benchmarking.src.test_types.base.DatasetGenerator"), patch(
                "benchmarking.src.test_types.base.SimpleKafkaProduceHandler"
            ):
                sut = BaseTest(test_total_message_count, test_is_interval_based)

    def test_failed_negative_message_count(self):
        # Arrange
        test_total_message_count = -10
        test_is_interval_based = True

        # Act
        with self.assertRaises(ValueError):
            with patch("benchmarking.src.test_types.base.DatasetGenerator"), patch(
                "benchmarking.src.test_types.base.SimpleKafkaProduceHandler"
            ):
                sut = BaseTest(test_total_message_count, test_is_interval_based)
