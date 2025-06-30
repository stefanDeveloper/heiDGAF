import pytest
import yaml
import os
import unittest
from src.zeek.zeek_config_handler import ZeekConfigurationHandler
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
                {"hostname": "broker1", "port": 9092, "node_ip": "123.123.123.123"},
                {"hostname": "broker2", "port": 9093, "node_ip": "123.123.123.123"}
            ]
        },
        "pipeline": {
            "zeek": {
                "sensors": {
                    "sensor-01": {
                        "static_analysis": False,
                        "protocol_to_topic": [
                            {"dns": "dns-topic"},
                            {"http": "http-topic"}
                        ],
                        "interfaces": [
                            "eth0",
                            "dummy"
                        ]
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
                {"hostname": "broker1", "port": 9092, "node_ip": "123.123.123.123"},
                {"hostname": "broker2", "port": 9093, "node_ip": "123.123.123.123"}
            ],
        },
        "pipeline": {
            "zeek": {
                "sensors": {
                    "sensor-01": {
                        "static_analysis": True,
                        "protocol_to_topic": [
                            {"all": "pipeline-logserver-in"}
                            ]
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
        self.assertEqual(handler.kafka_brokers[0], "123.123.123.123:9092")
        self.assertEqual(handler.kafka_brokers[1], "123.123.123.123:9093")

        # Topic mapping
        expected = {"http": "http-topic", "dns": "dns-topic"}
        self.assertEqual(handler.potocol_to_topic_configurations, expected)

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_hostname_env(self):
        with self.assertRaises(Exception) as context:
            ZeekConfigurationHandler(self.config_dict)
        self.assertIn("CONTAINER_NAME env. variable not found", str(context.exception))

class TestZeekConfiguration(unittest.TestCase):

    def setUp(self):
        # self.kafka_brokers = ["broker1:9092", "broker2:9093"]
        self.container_name = "sensor-01"
        self.config_dict_all_protocols = yaml.safe_load(get_yaml_config_all_protocols())
        self.config_restricted_protocols = yaml.safe_load(get_yaml_config_string())

    @patch.dict(os.environ, {"CONTAINER_NAME": "sensor-01"})
    def test_configuration_all_protocols(self):
        handler = ZeekConfigurationHandler(self.config_dict_all_protocols)
        handler.potocol_to_topic_configurations = {"all": "zeek-logs"}
        handler.base_config_location = tempfile.mktemp()
        handler.network_interface = "eth0"

        handler.configure()

        with open(handler.base_config_location, "r") as f:
            content = f.read()

        self.assertIn("@load packages/zeek-kafka", content)
        self.assertIn("redef Kafka::send_all_active_logs = T;", content)
        self.assertIn("zeek-logs", content)
        self.assertIn("123.123.123.123:9092,123.123.123.123:9093", content)

    @patch.dict(os.environ, {"CONTAINER_NAME": "sensor-01"})
    def test_configure_per_protocol_mode(self):
        handler = ZeekConfigurationHandler(self.config_restricted_protocols)
        handler.potocol_to_topic_configurations = {
            "http": "http-topic",
            "dns": "dns-topic"
        }
        handler.base_config_location = tempfile.mktemp()
        handler.zeek_node_config_path = tempfile.mktemp()
        handler.network_interface = "eth1"
        handler.zeek_node_config_template = "./tests/zeek/base_node.cfg"
        handler.configure()

        with open(handler.base_config_location, "r") as f:
            content = f.read()

        self.assertIn("@load packages/zeek-kafka", content)
        self.assertIn("Log::add_filter(HTTP::LOG", content)
        self.assertIn("Log::add_filter(DNS::LOG", content)
        self.assertIn("http-topic", content)
        self.assertIn("123.123.123.123:9092", content)

if __name__ == "__main__":
    unittest.main()
