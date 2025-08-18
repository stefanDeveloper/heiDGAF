import click
import sys
import yaml
import shutil
import os
from src.zeek.zeek_analysis_handler import ZeekAnalysisHandler
from src.zeek.zeek_config_handler import ZeekConfigurationHandler

sys.path.append(os.getcwd())
from src.base.log_config import get_logger
logger = get_logger("zeek.sensor")

@click.command()
@click.option(
    "-c",
    "--config",
    "configuration_file_path",
    required=True,
    type=click.File(mode="r"),
    help="Path to the configuration file location",
)
@click.option(
    "--zeek-config-location",
    "zeek_config_location",
    help=(
        "Overrides the default configuration location of Zeek under /usr/local/zeek/share/zeek/site/local.zeek"
    ),
)
def setup_zeek(configuration_file_path, zeek_config_location):
    """
    Configure and start Zeek analysis based on pipeline configuration.
    
    This is the main entry point for the Zeek configuration and analysis process.
    It handles the complete workflow from configuration setup to analysis execution.
    
    The function:
    1. Manages Zeek configuration backups to ensure clean setup between runs
    2. Parses the pipeline configuration file
    3. Configures Zeek using the specified or default configuration location
    4. Starts analysis in the appropriate mode (static or network)
    
    Args:
        configuration_file_path: File object pointing to the pipeline configuration
            YAML file that defines sensor settings, Kafka brokers, and other parameters
        zeek_config_location: Optional path to override the default Zeek configuration
            location. If not provided, uses /usr/local/zeek/share/zeek/site/local.zeek
    
    Workflow:
        1. On first run: Backs up the default Zeek configuration
        2. On subsequent runs: Restores the backed-up configuration to ensure a clean state
        3. Parses the YAML configuration file
        4. Configures Zeek using ZeekConfigurationHandler
        5. Starts analysis using ZeekAnalysisHandler in the mode specified by the config
    
    Raises:
        yaml.YAMLError: If the configuration file is not valid YAML
        Exception: If required environment variables (like CONTAINER_NAME) are missing
    """
    default_zeek_config_location = "/usr/local/zeek/share/zeek/site/local.zeek"
    default_zeek_config_backup_location = "/opt/local.zeek_backup"
    initial_zeek_setup: bool = (
        False if os.path.isfile(default_zeek_config_backup_location) else True
    )
    logger.info(f"initial setup: {initial_zeek_setup}")
    if initial_zeek_setup:
        logger.info("Backup default config")
        shutil.copy2(default_zeek_config_location, default_zeek_config_backup_location)
    else:
        logger.info("Restore default config")
        shutil.copy2(default_zeek_config_backup_location, default_zeek_config_location)

    configuration_file_content = configuration_file_path.read()
    try:
        data = yaml.safe_load(configuration_file_content)
    except yaml.YAMLError as e:
        logger.error("Error parsing the config file. Is this proper yaml?")
        raise(e)

    if zeek_config_location is None:
        zeek_config_location = default_zeek_config_location
        zeekConfigHandler = ZeekConfigurationHandler(data, default_zeek_config_location)
    else:
        zeekConfigHandler = ZeekConfigurationHandler(data, zeek_config_location)

    zeekConfigHandler.configure()
    logger.info("configured zeek")
    zeekAnalysisHandler = ZeekAnalysisHandler(
        zeek_config_location, zeekConfigHandler.zeek_log_location
    )
    logger.info("starting analysis...")
    zeekAnalysisHandler.start_analysis(zeekConfigHandler.is_analysis_static)


if __name__ == "__main__": # pragma: no cover
    setup_zeek()
