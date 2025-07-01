import sys
import os
import shutil
sys.path.append(os.getcwd())
from src.base.log_config import get_logger

logger = get_logger("zeek.sensor")
class ZeekConfigurationHandler():
    def __init__(
            self, 
            configuration_dict: dict, 
            zeek_config_location: str = "/usr/local/zeek/share/zeek/site/local.zeek",
            zeek_node_config_template: str = "/opt/src/zeek/base_node.cfg",
            zeek_log_location: str = "/usr/local/zeek/log/zeek.log",
        ):      
        logger.info(f"Setting up Zeek configuration...")
        self.base_config_location = zeek_config_location
        self.zeek_node_config_template = zeek_node_config_template
        self.zeek_node_config_path: str = "/usr/local/zeek/etc/node.cfg"
        self.zeek_log_location = zeek_log_location
        
        self.container_name = os.getenv("CONTAINER_NAME", None)
        if self.container_name is None:
            logger.error("CONTAINER_NAME ENV variable could not be found. Aborting configuration...") 
            raise Exception("CONTAINER_NAME env. variable not found.")
        
        configured_kafka_brokers = configuration_dict["environment"]["kafka_brokers"]
        #configured_kafka_topic = configuration_dict["environment"]["kafka_topics"]["pipeline"]["zeek_to_logserver"]
        zeek_sensor_configuration = configuration_dict["pipeline"]["zeek"]["sensors"][self.container_name]
        
        if "static_analysis" in zeek_sensor_configuration.keys() and zeek_sensor_configuration["static_analysis"]:
            self.is_analysis_static = True
        else:
            self.is_analysis_static = False
            try:
                self.network_interfaces = zeek_sensor_configuration["interfaces"]
            except Exception as e:
                logger.error(e)
                logger.error("Could not parse configuration for zeek sensor, as the 'interfaces' parameter is not specified")
        
        self.potocol_to_topic_configurations = {str(protocol):str(topic)  for protocol_topic_dict in zeek_sensor_configuration["protocol_to_topic"] for protocol, topic in protocol_topic_dict.items()}
        self.kafka_brokers = [ f"{broker['node_ip']}:{broker['port']}" for broker in configured_kafka_brokers ]
        logger.info(f"Succesfully parse config.yaml")

    def configure(self):
        logger.info(f"configuring Zeek...")
        if not self.is_analysis_static:
            self.template_and_copy_node_config()
        self.create_plugin_configuration()
        self.create_additional_dns_configuration()
    
    def create_additional_dns_configuration(self):
        dns_payload_extension = [
            "\n@load base/protocols/dns\n",
            "module DNS;\n",
            "redef record DNS::Info += {\n",
            "    dns_payload_len_bytes: count &optional &log;\n",
            "};\n",
            "event dns_message(c: connection, is_query: bool, msg: dns_msg, len: count)\n",
            "{\n",
            "    if ( c?$dns ) {\n",
            "        c$dns$dns_payload_len_bytes = len;\n",
            "    }\n",
            "}\n"
        ]

        with open(self.base_config_location, "a") as f:
            f.writelines(dns_payload_extension)
        logger.info("Appended DNS payload length extension to Zeek config")
        
    
    def create_plugin_configuration(self):
        if "all" in self.potocol_to_topic_configurations:
            config_lines = [
                "@load packages/zeek-kafka\n",
                "redef Kafka::send_all_active_logs = T;\n",
                f"redef Kafka::topic_name = \"{self.potocol_to_topic_configurations['all']}\";\n",
                "redef Kafka::kafka_conf = table(\n",
                f'    ["metadata.broker.list"] = "{",".join(self.kafka_brokers)}"\n',
                ");\n"
            ]
        else:
            config_lines = [
                '@load packages/zeek-kafka\n',
                'redef Kafka::topic_name = "";\n',
                f'redef Kafka::kafka_conf = table(\n'
                f'  ["metadata.broker.list"] = "{",".join(self.kafka_brokers)}");\n',
                'redef Kafka::tag_json = T;\n',
                'event zeek_init() &priority=-10\n',
                '{\n'
            ]

            for protocol, topic in self.potocol_to_topic_configurations.items():
                filter_block = f"""
                    local {protocol}_filter: Log::Filter = [
                        $name = "kafka-{protocol}",
                        $writer = Log::WRITER_KAFKAWRITER,
                        $path = "{topic}"
                    ];
                    Log::add_filter({protocol.upper()}::LOG, {protocol}_filter);\n
                    
                """
                config_lines.append(filter_block)
            config_lines.append('\n}')   
                  
        with open(self.base_config_location, "a") as f:
            f.writelines(config_lines)
        logger.info("Wrote kafka zeek plugin configuration to file")
            
    def create_worker_configurations_for_interfaces(self):
        worker_configuration_lines = []
        for network_interface in self.network_interfaces:
            worker_configuration_lines.extend([
                f"[zeek-{network_interface}]\n",
                "type=worker\n",
                "host=localhost\n"
            ])
        return worker_configuration_lines
    
    def template_and_copy_node_config(self):
        shutil.copy2(self.zeek_node_config_template, self.zeek_node_config_path)
        configuration_lines = self.create_worker_configurations_for_interfaces()
        with open(self.zeek_node_config_path, "a") as f:
            f.writelines(configuration_lines)