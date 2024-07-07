import unittest

from src.base.kafka_handler import KafkaHandler, KAFKA_BROKERS, CONSUMER_GROUP_ID


class TestInit(unittest.TestCase):
    def test_init(self):
        sut = KafkaHandler()

        self.assertIsNone(sut.consumer)
        self.assertEqual(KAFKA_BROKERS, sut.kafka_brokers)
        self.assertEqual(CONSUMER_GROUP_ID, sut.consumer_group_id)


if __name__ == "__main__":
    unittest.main()
