import unittest
import uuid

import marshmallow_dataclass

from src.base.data_classes.batch import Batch


class TestClearData(unittest.TestCase):
    def test_clear_data_with_existing_data(self):
        batch_tree_row_id=f"{str(uuid.uuid4())}-{str(uuid.uuid4())}"
        json_data = {
            "batch_tree_row_id": batch_tree_row_id,
            "batch_id": str(uuid.uuid4()),
            "begin_timestamp": "2024-05-21T08:31:27",
            "end_timestamp": "2024-05-21T08:31:29",
            "data": [
                {
                    "timestamp": "2024-05-21T08:31:28.119",
                    "status": "NOERROR",
                    "src_ip": "192.168.0.105",
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


if __name__ == "__main__":
    unittest.main()
