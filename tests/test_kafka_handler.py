import unittest

from heidgaf_core.kafka_handler import KafkaHandler


class TestInit(unittest.TestCase):
    def test_init(self):
        handler_instance = KafkaHandler()

        self.assertIsNone(handler_instance.consumer)
        self.assertEqual("localhost:9092,localhost:9093,localhost:9094",
                         handler_instance.brokers)


if __name__ == '__main__':
    unittest.main()
