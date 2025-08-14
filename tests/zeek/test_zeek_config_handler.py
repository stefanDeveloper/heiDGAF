import unittest
from unittest.mock import patch, MagicMock, mock_open, call
import tempfile
import os
from src.zeek.zeek_config_handler import ZeekConfigurationHandler

class TestZeekConfigHandler(unittest.TestCase):
    def setUp(self):
        os.environ["CONTAINER_NAME"] = "ZEEK_TEST_CONTAINER"
        self.mock_config = {
            "pipeline": {
                "zeek": {
                    "sensors": {
                        "ZEEK_TEST_CONTAINER": {
                        "static_analysis": True,
                        "protocols": [
                            "http",
                            "dns"
                        ],
                        "interfaces": [
                            "enx84ba5960ffe6"
                        ]
                        }
                    }
                }
            },
            "environment": {
                "kafka_brokers": [
                {
                    "hostname": "kafka1",
                    "port": 8097,
                    "node_ip": "192.168.175.69"
                },
                {
                    "hostname": "kafka2",
                    "port": 8098,
                    "node_ip": "192.168.175.69"
                },
                {
                    "hostname": "kafka3",
                    "port": 8099,
                    "node_ip": "192.168.175.69"
                }
                ],
                "kafka_topics_prefix": {
                    "pipeline": {
                        "logserver_in": "pipeline-logserver_in",
                        "logserver_to_collector": "pipeline-logserver_to_collector",
                        "batch_sender_to_prefilter": "pipeline-batch_sender_to_prefilter",
                        "prefilter_to_inspector": "pipeline-prefilter_to_inspector",
                        "inspector_to_detector": "pipeline-inspector_to_detector"
                    }
                }
            }
        }

        
    def tearDown(self):
        del os.environ["CONTAINER_NAME"]

    def test_default_initialization_static_analysis(self):
        handler = ZeekConfigurationHandler(self.mock_config)
        # Assert
        self.assertEqual(handler.base_config_location, "/usr/local/zeek/share/zeek/site/local.zeek")
        self.assertEqual(handler.zeek_log_location, "/usr/local/zeek/log/zeek.log")
        self.assertEqual(handler.is_analysis_static,True)
        
    def test_default_initialization_network_analysis(self):
        self.mock_config["pipeline"]["zeek"]["sensors"]["ZEEK_TEST_CONTAINER"]["static_analysis"] = False
        handler = ZeekConfigurationHandler(self.mock_config)
        # Assert
        self.assertEqual(handler.base_config_location, "/usr/local/zeek/share/zeek/site/local.zeek")
        self.assertEqual(handler.zeek_log_location, "/usr/local/zeek/log/zeek.log")
        self.assertEqual(handler.is_analysis_static,False)
        self.assertEqual(handler.network_interfaces, ["enx84ba5960ffe6"])

    @patch("builtins.open", new_callable=unittest.mock.mock_open)
    @patch("os.path.exists", return_value=True)
    def test_configure_default_mode(self, mock_exists, mock_open):
        handler = ZeekConfigurationHandler(self.mock_config)
        # Arrange
        pipline_in_topic_prefix = self.mock_config["environment"]["kafka_topics_prefix"]["pipeline"]["logserver_in"]
        handler.zeek_node_config_path = "/tmp/node.cfg"
        handler.potocol_to_topic_configurations = {
            "http": "http-topic",
            "dns": "dns-topic"
        }
        handler.kafka_brokers = ["localhost:9092"]

        # Act
        handler.configure()

        # Assert
        mock_open.assert_any_call("/usr/local/zeek/share/zeek/site/local.zeek", "a")
        handle = mock_open()
        
        args, kwargs = handle.writelines.call_args_list[0]
        written_lines = "".join(args[0]) 
        
        expected_lines = [
            '@load packages/zeek-kafka',
            'Log::add_filter(CustomHTTP::LOG, http_filter)',
            'Log::add_filter(CustomDNS::LOG, dns_filter)',
            '["metadata.broker.list"] = "localhost:9092"',
            f"{pipline_in_topic_prefix}-dns",
            f"{pipline_in_topic_prefix}-http"
            
        ]
        for expected_line in expected_lines:
            self.assertIn(expected_line,written_lines)


    @patch("src.zeek.zeek_config_handler.glob.glob")
    def test_append_additional_configurations(self, mock_glob):
        # Arrange
        mock_glob.return_value = ["/opt/src/zeek/additional_configs/custom.zeek"]
        
        # Create a mock that returns different content for different files
        m = mock_open(read_data="@load custom-script\n")
        
        with patch("builtins.open", m, create=True):
            handler = ZeekConfigurationHandler(self.mock_config)
            handler.base_config_location = "/mock/zeek.cfg"
            
            # Act
            handler.append_additional_configurations()
            
            # Assert
            mock_glob.assert_called_once_with("/opt/src/zeek/additional_configs/*.zeek")
            
            # Check the content that was written to the base config
            handle = m()
            
        
            args, kwargs = handle.writelines.call_args_list[0]
            written_lines = "".join(args[0])  
            
            self.assertIn("@load custom-script", written_lines)
            self.assertEqual(handle.write.call_count, 1)
            self.assertEqual(handle.writelines.call_count, 1)

    def test_create_plugin_configuration(self):
        # Arrange
        mock_config = {
            "environment": {
                "kafka_brokers": [
                    {"hostname": "kafka1", "port": 8097, "node_ip": "192.168.175.69"},
                    {"hostname": "kafka2", "port": 8098, "node_ip": "192.168.175.70"}
                ],
                "kafka_topics_prefix": {
                    "pipeline": {
                        "logserver_in": "pipeline-logserver_in"
                    }
                }
            },
            "pipeline": {
                "zeek": {
                    "sensors": {
                        "test_container": {
                            "protocols": ["http", "dns"]
                        }
                    }
                }
            }
        }
        os.environ["CONTAINER_NAME"] = "test_container"
        
        handler = ZeekConfigurationHandler(mock_config)
        handler.base_config_location = "/mock/zeek.cfg"
        
        # Create a mock for the file
        m = mock_open()
        
        # Act
        with patch("builtins.open", m):
            handler.create_plugin_configuration()
        
        handle = m()
        # Assert
        m.assert_any_call("/mock/zeek.cfg", "a")
        
        args, kwargs = handle.writelines.call_args_list[0]  # args is a tuple of positional args
        written_lines = "".join(args[0])  # the iterable passed to writelines concatenated to a single string
        
        expected_lines = [
            "@load packages/zeek-kafka",
            "redef Kafka::topic_name = \"\"",
            "192.168.175.69:8097,192.168.175.70:8098",
            "pipeline-logserver_in-http",
            "CustomDNS::LOG",
            "CustomHTTP::LOG",
            "pipeline-logserver_in-dns",            
        ]
        for expected_line in expected_lines:
            self.assertIn(expected_line,written_lines)



    @patch("builtins.open", new_callable=mock_open)
    @patch("src.zeek.zeek_config_handler.shutil.copy2")
    def test_template_and_copy_node_config(self, mock_shutil_copy, mock_open_file):
        # Arrange
        handler = ZeekConfigurationHandler(self.mock_config)
        handler.is_analysis_static = False
        handler.network_interfaces = ["eth0", "dummy"]
        
        # Act
        handler.template_and_copy_node_config()
        
        # Assert
        mock_shutil_copy.assert_called_once_with(
            "/opt/src/zeek/base_node.cfg", 
            "/usr/local/zeek/etc/node.cfg"
        )
        
        expected_worker_config = [
            "[zeek-eth0]\n", "type=worker\n", "host=localhost\n",
            "[zeek-dummy]\n", "type=worker\n", "host=localhost\n"
        ]
        mock_open_file.assert_called_once_with("/usr/local/zeek/etc/node.cfg", "a")
        mock_open_file().writelines.assert_called_once_with(expected_worker_config)