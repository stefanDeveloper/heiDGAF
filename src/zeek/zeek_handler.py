import click
import sys
import yaml
import os
from src.zeek.zeek_analysis_handler import ZeekAnalysisHandler
from src.zeek.zeek_config_handler import ZeekConfigurationHandler
sys.path.append(os.getcwd())
from src.base.log_config import get_logger

logger = get_logger("zeek.sensor")


# Connect to kafka using node IP and PORT from expsed kafka ports
        
@click.command()
@click.option(
    "-c",
    "--config",
    "configuration_file_path",
    required=True,
    type=click.File(mode="r"),
    help="Path to the configuration file location"
)
@click.option(
    "--zeek-config-location",
    "zeek_config_location",
    help=("Overrides the default configuration location of Zeek under /usr/local/zeek/share/zeek/site/local.zeek")
)

   
def setup_zeek(configuration_file_path, zeek_config_location):
    default_zeek_config_location = "/usr/local/zeek/share/zeek/site/local.zeek"
    configuration_file_content = configuration_file_path.read()
    try:
        data = yaml.safe_load(configuration_file_content)
    except yaml.YAMLError as e:
        logger.error("Erro parsing the config file. Is this proper yaml?")
        return
    
    if zeek_config_location is None:
        zeek_config_location = default_zeek_config_location
        zeekConfigHandler = ZeekConfigurationHandler(data, default_zeek_config_location)
    else:
        zeekConfigHandler = ZeekConfigurationHandler(data, zeek_config_location)        
        
    zeekConfigHandler.configure()
    logger.info("configured zeek")
    zeekAnalysisHandler = ZeekAnalysisHandler(zeek_config_location, zeekConfigHandler.zeek_log_location)
    logger.info("starting analysis...")
    zeekAnalysisHandler.start_analysis(zeekConfigHandler.is_analysis_static)
    
if __name__ == "__main__":
    setup_zeek()
    