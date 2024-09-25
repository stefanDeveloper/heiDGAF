import unittest
from unittest.mock import MagicMock, patch

import marshmallow_dataclass

from src.base import Batch
from src.detector.detector import Detector, main


class TestClearData(unittest.TestCase):
    def test_clear_data_with_existing_data(self):
        json_data = {
            "begin_timestamp": "2024-05-21T08:31:27.000000Z",
            "end_timestamp": "2024-05-21T08:31:29.000000Z",
            "data": [
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

        base_schema = marshmallow_dataclass.class_schema(Batch)()
        data_batch = base_schema.load(json_data)
        json_data_deserialized = base_schema.dump(data_batch)
        self.assertEqual(json_data_deserialized, json_data)
