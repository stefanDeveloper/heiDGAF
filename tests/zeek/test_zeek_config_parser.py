import pytest
import yaml
import os
import unittest
from zeek.zeek_handler import ZeekConfigurationHandler, configure_zeek
from click.testing import CliRunner
import unittest
import os
import tempfile
import yaml
from unittest.mock import patch

def get_yaml_config_string():
    config = {
        "environment": {
            "kafka_brokers": [
                {"hostname": "broker1", "port": 9092},
                {"hostname": "broker2", "port": 9093}
            ],
            "kafka_topics": {
                "pipeline": {
                    "zeek_to_logserver": "zeek-logs"
                }
            }
        },
        "pipeline": {
            "zeek": {
                "sensors": {
                    "sensor-01": {
                        "protocols": ["http", "dns"],
                        "interface": "eth0"
                    }
                }
            }
        }
    }
    return yaml.dump(config)
def get_yaml_config_all_protocols():
    config = {
        "environment": {
            "kafka_brokers": [
                {"hostname": "broker1", "port": 9092},
                {"hostname": "broker2", "port": 9093}
            ],
            "kafka_topics": {
                "pipeline": {
                    "zeek_to_logserver": "zeek-logs"
                }
            }
        },
        "pipeline": {
            "zeek": {
                "sensors": {
                    "sensor-01": {
                        "protocols": ["all"],
                        "interface": "eth0"
                    }
                }
            }
        }
    }
    return yaml.dump(config)

class TestZeekConfigurationHandler(unittest.TestCase):

    def setUp(self):
        self.yaml_string = get_yaml_config_string()
        self.config_dict = yaml.safe_load(self.yaml_string)

    @patch.dict(os.environ, {"CONTAINER_NAME": "sensor-01"})
    def test_zeek_config_handler(self):
        handler = ZeekConfigurationHandler(self.config_dict)

        self.assertEqual(len(handler.kafka_brokers), 2)
        self.assertEqual(handler.kafka_brokers[0], "broker1:9092")
        self.assertEqual(handler.kafka_brokers[1], "broker2:9093")

        # Topic mapping
        expected = {"http": "zeek-logs", "dns": "zeek-logs"}
        self.assertEqual(handler.potocol_to_topic_configurations, expected)

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_hostname_env(self):
        with self.assertRaises(Exception) as context:
            ZeekConfigurationHandler(self.config_dict)
        self.assertIn("CONTAINER_NAME env. variable not found", str(context.exception))

class TestZeekConfiguration(unittest.TestCase):

    def setUp(self):
        self.kafka_brokers = ["broker1:9092", "broker2:9093"]
        self.container_name = "sensor-01"
        self.config_dict_all_protocols = yaml.safe_load(get_yaml_config_all_protocols())
        self.config_restricted_protocols = yaml.safe_load(get_yaml_config_string())

    @patch.dict(os.environ, {}, clear=True)
    def test_setup_network_interface_for_analysis_sets_env_var(self):
        handler = ZeekConfigurationHandler.__new__(ZeekConfigurationHandler)
        handler.network_interface = "eth0"
        handler.setup_network_interface_for_analysis()

        self.assertEqual(os.environ["NETWORK_INTERFACE"], "eth0")

    @patch.dict(os.environ, {"CONTAINER_NAME": "sensor-01"})
    def test_configuration_all_protocols(self):
        handler = ZeekConfigurationHandler(self.config_dict_all_protocols)
        handler.kafka_brokers = ["broker1:9092", "broker2:9093"]
        handler.potocol_to_topic_configurations = {"all": "zeek-logs"}
        handler.base_config_location = tempfile.mktemp()
        handler.network_interface = "eth0"

        handler.configure()

        with open(handler.base_config_location, "r") as f:
            content = f.read()

        self.assertIn("@load packages/zeek-kafka", content)
        self.assertIn("redef Kafka::send_all_active_logs = T;", content)
        self.assertIn("zeek-logs", content)
        self.assertIn("broker1:9092,broker2:9093", content)

    @patch.dict(os.environ, {"CONTAINER_NAME": "sensor-01"})
    def test_configure_per_protocol_mode(self):
        handler = ZeekConfigurationHandler(self.config_restricted_protocols)
        handler.kafka_brokers = ["broker1:9092"]
        handler.potocol_to_topic_configurations = {
            "http": "zeek-logs",
            "dns": "zeek-logs"
        }
        handler.base_config_location = tempfile.mktemp()
        handler.network_interface = "eth1"

        handler.configure()

        with open(handler.base_config_location, "r") as f:
            content = f.read()

        self.assertIn("@load packages/zeek-kafka", content)
        self.assertIn("Log::add_filter(HTTP::LOG", content)
        self.assertIn("Log::add_filter(DNS::LOG", content)
        self.assertIn("zeek-logs", content)
        self.assertIn("broker1:9092", content)

if __name__ == "__main__":
    unittest.main()
