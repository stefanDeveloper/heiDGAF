import unittest
from unittest.mock import MagicMock, patch

import marshmallow_dataclass

from src.base import Batch
from src.detector.detector import Detector, main


class TestClearData(unittest.TestCase):
    def test_clear_data_with_existing_data(self):
        json_data = {
            "begin_timestamp": "2024-05-21T08:31:27.000Z",
            "end_timestamp": "2024-05-21T08:31:29.000Z",
            "messages": [
                {
                    "timestamp": "2024-05-21T08:31:28.119Z",
                    "status": "NOERROR",
                    "client_ip": "192.168.0.105",
                    "dns_ip": "8.8.8.8",
                    "host_domain_name": "www.heidelberg-botanik.de",
                    "record_type": "A",
                    "response_ip": "b937:2f2e:2c1c:82a:33ad:9e59:ceb9:8e1",
                    "size": "150b",
                }
            ],
        }

        city_schema = marshmallow_dataclass.class_schema(Batch)()
        data_batch = city_schema.load(json_data)
        print(data_batch)
        json_data_deserialized = city_schema.dump(data_batch)
        print(json_data_deserialized)
        self.assertEqual(json_data_deserialized, json_data)
