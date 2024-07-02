import unittest

from heidgaf_core.kafka_handler import KafkaHandler

BROKER_1 = "172.27.0.3:8097"
BROKER_2 = "172.27.0.4:8098"
BROKER_3 = "172.27.0.5:8099"


class TestInit(unittest.TestCase):
    def test_init(self):
        handler_instance = KafkaHandler()

        self.assertIsNone(handler_instance.consumer)
        self.assertEqual(f"{BROKER_1},{BROKER_2},{BROKER_3}", handler_instance.brokers)


if __name__ == "__main__":
    unittest.main()
