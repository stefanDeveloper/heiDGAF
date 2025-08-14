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


# Connect to kafka using node IP and PORT from expsed kafka ports


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
    default_zeek_config_location = "/usr/local/zeek/share/zeek/site/local.zeek"
    default_zeek_config_backup_location = "/opt/local.zeek_backup"
    initial_zeek_setup: bool = (
        False if os.path.isfile(default_zeek_config_backup_location) else True
    )
    logger.info(f"initial setup: {initial_zeek_setup}")
    # backup the default config if first use of a container
    if initial_zeek_setup:
        logger.info("Backup default config")
        shutil.copy2(default_zeek_config_location, default_zeek_config_backup_location)
    # if it is not the first use, restore the backuped version, otherwise the configured version
    # gets configured another time
    else:
        logger.info("Restore default config")
        shutil.copy2(default_zeek_config_backup_location, default_zeek_config_location)
    # no need to check if proper file, as this is already checked with @click.option type=File
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


if __name__ == "__main__":

    setup_zeek()
