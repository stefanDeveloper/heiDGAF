import unittest
from unittest.mock import patch

from src.base.kafka_handler import KafkaHandler


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
    def test_init(self):
        expected_brokers = "127.0.0.1:9999,127.0.0.2:9998,127.0.0.3:9997"

        sut = KafkaHandler()

        self.assertIsNone(sut.consumer)
        self.assertEqual(expected_brokers, sut.brokers)


if __name__ == "__main__":
    unittest.main()
