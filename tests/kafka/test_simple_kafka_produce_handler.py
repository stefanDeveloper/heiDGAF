import unittest
from unittest.mock import patch, Mock

from src.base.kafka_handler import SimpleKafkaProduceHandler
from src.base.utils import kafka_delivery_report


class TestInit(unittest.TestCase):
    @patch(
        "src.base.kafka_handler.KAFKA_BROKERS",
        [
            {
                "hostname": "127.0.0.1",
                "port": 9999,
            },
            {
                "hostname": "127.0.0.2",
                "port": 9998,
            },
            {
                "hostname": "127.0.0.3",
                "port": 9997,
            },
        ],
    )
    def test_successful(self):
        # Arrange
        expected_conf = {
            "bootstrap.servers": "127.0.0.1:9999,127.0.0.2:9998,127.0.0.3:9997",
            "enable.idempotence": False,
            "acks": "1",
            'message.max.bytes': 1000000000,
        }

        # Act
        with patch("src.base.kafka_handler.Producer") as mock_producer:
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance

            sut = SimpleKafkaProduceHandler()

        # Assert
        self.assertEqual("127.0.0.1:9999,127.0.0.2:9998,127.0.0.3:9997", sut.brokers)
        self.assertIsNone(sut.consumer)
        mock_producer.assert_called_once_with(expected_conf)


class TestProduce(unittest.TestCase):
    def test_with_data(self):
        with patch("src.base.kafka_handler.Producer") as mock_producer:
            # Arrange
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance

            sut = SimpleKafkaProduceHandler()

            # Act
            sut.produce("test_topic", "test_data")

        # Assert
        mock_producer_instance.flush.assert_called_once()
        mock_producer_instance.produce.assert_called_once_with(
            topic="test_topic",
            key=None,
            value="test_data",
            callback=kafka_delivery_report,
        )

    def test_without_data(self):
        with patch("src.base.kafka_handler.Producer") as mock_producer:
            # Arrange
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance

            sut = SimpleKafkaProduceHandler()

            # Act
            sut.produce("test_topic", "")

        # Assert
        mock_producer_instance.flush.assert_not_called()
        mock_producer_instance.produce.assert_not_called()


if __name__ == "__main__":
    unittest.main()
