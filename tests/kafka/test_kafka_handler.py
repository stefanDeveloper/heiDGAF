import unittest

from src.base.kafka_handler import KafkaHandler


class TestInit(unittest.TestCase):
    def test_init(self):
        sut = KafkaHandler()

        self.assertIsNone(sut.consumer)


if __name__ == "__main__":
    unittest.main()
