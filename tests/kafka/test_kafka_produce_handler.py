import unittest
from unittest.mock import patch, Mock

from src.base.kafka_handler import KafkaProduceHandler


class TestInit(unittest.TestCase):
    def test_successful(self):
        # Arrange
        conf = "test_conf"

        # Act
        with patch("src.base.kafka_handler.Producer") as mock_producer:
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance

            sut = KafkaProduceHandler(conf)

        # Assert
        self.assertEqual(None, sut.consumer)
        self.assertEqual(mock_producer_instance, sut.producer)
        mock_producer.assert_called_once_with(conf)


class TestProduce(unittest.TestCase):
    @patch("src.base.kafka_handler.Producer")
    def test_not_implemented(self, mock_producer):
        # Arrange
        sut = KafkaProduceHandler("test_conf")

        # Act and Assert
        with self.assertRaises(NotImplementedError):
            sut.produce()


class TestDel(unittest.TestCase):
    @patch("src.base.kafka_handler.Producer")
    def test_not_implemented(self, mock_producer):
        # Arrange
        mock_producer_instance = Mock()
        mock_producer.return_value = mock_producer_instance
        sut = KafkaProduceHandler("test_conf")

        # Act
        del sut

        # Assert
        mock_producer_instance.flush.assert_called_once()


if __name__ == "__main__":
    unittest.main()
