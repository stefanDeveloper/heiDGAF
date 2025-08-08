import sys
import os
import shutil

sys.path.append(os.getcwd())
from src.base.log_config import get_logger
import glob

logger = get_logger("zeek.sensor")


class ZeekConfigurationHandler:
    def __init__(
        self,
        configuration_dict: dict,
        zeek_config_location: str = "/usr/local/zeek/share/zeek/site/local.zeek",
        zeek_node_config_template: str = "/opt/src/zeek/base_node.cfg",
        zeek_log_location: str = "/usr/local/zeek/log/zeek.log",
        additional_configurations: str = "/opt/src/zeek/additional_configs/",
    ):
        logger.info(f"Setting up Zeek configuration...")
        self.base_config_location = zeek_config_location
        self.additional_configurations = additional_configurations
        self.zeek_node_config_template = zeek_node_config_template
        self.zeek_node_config_path: str = "/usr/local/zeek/etc/node.cfg"
        self.zeek_log_location = zeek_log_location

        self.container_name = os.getenv("CONTAINER_NAME", None)
        if self.container_name is None:
            logger.error(
                "CONTAINER_NAME ENV variable could not be found. Aborting configuration..."
            )
            raise Exception("CONTAINER_NAME env. variable not found.")

        configured_kafka_brokers = configuration_dict["environment"]["kafka_brokers"]
        # configured_kafka_topic = configuration_dict["environment"]["kafka_topics"]["pipeline"]["zeek_to_logserver"]
        zeek_sensor_configuration = configuration_dict["pipeline"]["zeek"]["sensors"][
            self.container_name
        ]

        if (
            "static_analysis" in zeek_sensor_configuration.keys()
            and zeek_sensor_configuration["static_analysis"]
        ):
            self.is_analysis_static = True
        else:
            self.is_analysis_static = False
            try:
                self.network_interfaces = zeek_sensor_configuration["interfaces"]
            except Exception as e:
                logger.error(e)
                logger.error(
                    "Could not parse configuration for zeek sensor, as the 'interfaces' parameter is not specified"
                )

        self.kafka_topic_prefix = configuration_dict["environment"][
            "kafka_topics_prefix"
        ]["pipeline"]["logserver_in"]

        self.configured_protocols = [
            protocol for protocol in zeek_sensor_configuration["protocols"]
        ]
        self.kafka_brokers = [
            f"{broker['node_ip']}:{broker['port']}"
            for broker in configured_kafka_brokers
        ]
        logger.info(f"Succesfully parse config.yaml")

    def configure(self):
        logger.info(f"configuring Zeek...")
        if not self.is_analysis_static:
            self.template_and_copy_node_config()
        self.append_additional_configurations()
        self.create_plugin_configuration()

    def append_additional_configurations(self):
        config_files = find_files_in_dir(self.additional_configurations)
        with open(self.base_config_location, "a") as base_config:
            base_config.write("\n")
            for file in config_files:
                with open(file) as additional_config:
                    base_config.writelines(additional_config)

    def create_plugin_configuration(self):
        config_lines = [
            "@load packages/zeek-kafka\n",
            'redef Kafka::topic_name = "";\n',
            f"redef Kafka::kafka_conf = table(\n"
            f'  ["metadata.broker.list"] = "{",".join(self.kafka_brokers)}");\n',
            "redef Kafka::tag_json = F;\n",
            "event zeek_init() &priority=-10\n",
            "{\n",
        ]
        for protocol in self.configured_protocols:
            topic_name = f"{self.kafka_topic_prefix}-{protocol.lower()}"
            zeek_protocol_log_format = f"Custom{protocol.upper()}"
            kafka_writer_name = f"{protocol.lower()}_filter"
            filter_block = f"""
                local {kafka_writer_name}: Log::Filter = [
                    $name = "kafka-{kafka_writer_name}",
                    $writer = Log::WRITER_KAFKAWRITER,
                    $path = "{topic_name}"
                ];
                Log::add_filter({zeek_protocol_log_format}::LOG, {kafka_writer_name});\n
            """
            config_lines.append(filter_block)
        config_lines.append("\n}")

        with open(self.base_config_location, "a") as f:
            f.writelines(config_lines)
        logger.info("Wrote kafka zeek plugin configuration to file")

    def create_worker_configurations_for_interfaces(self):
        worker_configuration_lines = []
        for network_interface in self.network_interfaces:
            worker_configuration_lines.extend(
                [f"[zeek-{network_interface}]\n", "type=worker\n", "host=localhost\n"]
            )
        return worker_configuration_lines

    def template_and_copy_node_config(self):
        shutil.copy2(self.zeek_node_config_template, self.zeek_node_config_path)
        configuration_lines = self.create_worker_configurations_for_interfaces()
        with open(self.zeek_node_config_path, "a") as f:
            f.writelines(configuration_lines)


def find_files_in_dir(path):
    return glob.glob(os.path.join(path, "*.zeek"))
