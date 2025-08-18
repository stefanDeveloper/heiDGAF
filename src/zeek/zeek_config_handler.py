import sys
import os
import shutil

sys.path.append(os.getcwd())
from src.base.log_config import get_logger
import glob

logger = get_logger("zeek.sensor")


class ZeekConfigurationHandler:
    """
    Handles the configuration of Zeek sensors based on the pipeline configuration.
    
    This class is responsible for setting up Zeek to process network traffic according
    to the specified configuration. It configures the Zeek Kafka plugin, sets up worker
    nodes for network interfaces, and integrates additional custom configurations.
    
    The handler supports both static analysis (processing PCAP files) and network
    analysis (live traffic monitoring) modes, with configuration adapted to the
    specific sensor requirements defined in the pipeline configuration.
    
    Example:
        >>> config = {
        ...     "environment": {
        ...         "kafka_brokers": [{"hostname": "kafka1", "port": 9092, "node_ip": "192.168.1.100"}],
        ...         "kafka_topics_prefix": {"pipeline": {"logserver_in": "pipeline-logserver_in"}}
        ...     },
        ...     "pipeline": {
        ...         "zeek": {
        ...             "sensors": {
        ...                 "zeek1": {
        ...                     "static_analysis": True,
        ...                     "protocols": ["http", "dns"],
        ...                     "interfaces": ["eth0"]
        ...                 }
        ...             }
        ...         }
        ...     }
        ... }
        >>> os.environ["CONTAINER_NAME"] = "zeek1"
        >>> handler = ZeekConfigurationHandler(config)
        >>> handler.configure()
    """
    def __init__(
        self,
        configuration_dict: dict,
        zeek_config_location: str = "/usr/local/zeek/share/zeek/site/local.zeek",
        zeek_node_config_template: str = "/opt/src/zeek/base_node.cfg",
        zeek_log_location: str = "/usr/local/zeek/log/zeek.log",
        additional_configurations: str = "/opt/src/zeek/additional_configs/",
    ):
        """
        Initialize the Zeek configuration handler with the pipeline configuration.
        
        Args:
            configuration_dict: The complete pipeline configuration dictionary
                loaded from config.yaml, containing sensor, Kafka, and environment settings
            zeek_config_location: Path to the main Zeek configuration file where
                plugin configurations will be appended (default: standard Zeek location)
            zeek_node_config_template: Path to the template for node.cfg configuration
                (default: internal template in the project)
            zeek_log_location: Path where Zeek will write its log files
                (default: standard Zeek log location)
            additional_configurations: Directory containing additional Zeek configuration
                files that should be appended to the main configuration
        """
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
        """
        Execute the complete Zeek configuration process.
        
        This method orchestrates the entire configuration workflow:
        1. For network analysis mode: Sets up node configuration for network interfaces
        2. Appends any additional custom configurations
        3. Creates and writes the Kafka plugin configuration
        
        The method adapts the configuration based on whether the sensor is in
        static analysis mode (processing PCAP files) or network analysis mode
        (monitoring live traffic).
        
        Note:
            This is the main entry point for configuring Zeek. After calling this
            method, Zeek should be fully configured and ready to process traffic
            according to the pipeline specifications.
        """
        logger.info(f"configuring Zeek...")
        if not self.is_analysis_static:
            self.template_and_copy_node_config()
        self.append_additional_configurations()
        self.create_plugin_configuration()

    def append_additional_configurations(self):
        """
        Append custom configuration files to the main Zeek configuration.
        
        This method:
        1. Finds all *.zeek files in the additional configurations directory
        2. Appends their contents to the main Zeek configuration file
        
        Custom configuration files can be used to extend Zeek's functionality
        with custom scripts, event handlers, or protocol analyzers without
        modifying the core configuration.
        
        Example:
            If additional_configurations="/opt/src/zeek/additional_configs/"
            contains a file custom_http.zeek with content:
                @load base/protocols/http/main.zeek
                redef HTTP::default_accept_gzip = T;
            
            This content will be appended to the main Zeek configuration file.
        
        Note:
            The method adds a newline before appending each file to ensure
            proper separation between configuration sections.
        """
        config_files = find_files_in_dir(self.additional_configurations)
        with open(self.base_config_location, "a") as base_config:
            base_config.write("\n")
            for file in config_files:
                with open(file) as additional_config:
                    base_config.writelines(additional_config)

    def create_plugin_configuration(self):
        """
        Generate and write the Kafka plugin configuration for Zeek.
        
        This method:
        1. Creates the core Kafka plugin configuration
        2. Sets up topic mappings for each configured protocol
        3. Writes the complete configuration to the main Zeek configuration file
        
        The configuration directs Zeek to send processed log data to Kafka topics
        following the naming convention: {kafka_topic_prefix}-{protocol}
        
        """
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
        """
        Generate configuration lines for Zeek worker nodes.
        
        This method creates the configuration blocks needed for Zeek's cluster mode,
        where each network interface gets its own worker node.
        
        Returns:
            List[str]: Configuration lines that should be appended to node.cfg
            
        Example:
            For network_interfaces=["eth0", "dummy"], returns:
                [
                    "[zeek-eth0]\n",
                    "type=worker\n",
                    "host=localhost\n",
                    "[zeek-dummy]\n",
                    "type=worker\n",
                    "host=localhost\n"
                ]
        
        Note:
            This method is only called when in network analysis mode (not static analysis).
            Each worker is configured to run on the local host and process traffic
            from a specific network interface.
        """
        worker_configuration_lines = []
        for network_interface in self.network_interfaces:
            worker_configuration_lines.extend(
                [f"[zeek-{network_interface}]\n", "type=worker\n", "host=localhost\n"]
            )
        return worker_configuration_lines

    def template_and_copy_node_config(self):
        """
        Set up the node configuration for Zeek cluster mode.
        
        This method:
        1. Copies the node configuration template to Zeek's expected location
        2. Appends worker configurations for each network interface
        
        The node configuration (node.cfg) defines how Zeek should distribute
        processing across multiple worker processes, which is necessary for
        monitoring multiple network interfaces simultaneously.
        
        Note:
            This method is only called when in network analysis mode. Static
            analysis mode does not require worker configuration as it processes
            PCAP files sequentially.
        """
        shutil.copy2(self.zeek_node_config_template, self.zeek_node_config_path)
        configuration_lines = self.create_worker_configurations_for_interfaces()
        with open(self.zeek_node_config_path, "a") as f:
            f.writelines(configuration_lines)


def find_files_in_dir(path):  # pragma: no cover
    return glob.glob(os.path.join(path, "*.zeek"))
