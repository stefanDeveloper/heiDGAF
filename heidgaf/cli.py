import logging

import click
import torch

from heidgaf import CONTEXT_SETTINGS
from heidgaf.main import DNSAnalyzerPipeline, Detector, Separator
from heidgaf.models.lr import LogisticRegression
from heidgaf.train import DNSAnalyzerTraining
from heidgaf.version import __version__

try:
    import click
except ImportError:
    raise ImportError(
        "Please install Python dependencies: " "click, colorama (optional)."
    )


@click.version_option(version=__version__)
@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    logging.info("Starting heiDGAF CLI")


@cli.command(name="check_gpu")
def check_gpu():
    # setting device on GPU if available, else CPU
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    logging.info(f"Using device: {device}")
    if torch.cuda.is_available():
        logging.info("GPU detected")
        logging.info(f"\t{torch.cuda.get_device_name(0)}")

    if device.type == "cuda":
        logging.info("Memory Usage:")
        logging.info(
            f"\tAllocated: {round(torch.cuda.memory_allocated(0)/1024**3,1)} GB"
        )
        logging.info(
            f"\tCached:    {round(torch.cuda.memory_reserved(0)/1024**3,1)} GB"
        )


@cli.group(name="train", context_settings={"show_default": True})
def training_model():
    logging.info("Start training of model.")


@training_model.command(name="start")
def training_start():
    trainer = DNSAnalyzerTraining(
        model=LogisticRegression(input_dim=9, output_dim=1, epochs=5000)
    )
    trainer.train()


@cli.group(name="process", context_settings={"show_default": True})
def analyse():
    logging.info("Starts processing log lines of DNS traffic.")


@analyse.command(name="start")
@click.option(
    "-r", 
    "--read", 
    "input_dir", 
    required=True, 
    type=click.Path(), 
    help="Input directory or file for analyzing."
)
@click.option(
    "-dt",
    "--detector",
    "detector",
    type=click.Choice(Detector),
    default=Detector.THRESHOLDING,
    help="Sets the anomaly detector.",
)
@click.option(
    "-s",
    "--separator",
    "separator",
    type=click.STRING,
    default=Separator.COMMA.value,
    help="Separator type of input.",
)
@click.option(
    "--lag",
    "lag",
    type=click.FLOAT,
    default=15,
    help="Sets the anomaly detector lag.",
)
@click.option(
    "--influence",
    "influence",
    type=click.FLOAT,
    default=0.7,
    help="Sets the anomaly detector influence.",
)
@click.option(
    "--std",
    "n_standard_deviations",
    type=click.FLOAT,
    default=3,
    help="Sets the anomaly detector n standard deviation.",
)
@click.option(
    "--redis-host",
    "redis_host",
    type=click.STRING,
    default="localhost",
    help="Sets Redis host for caching results.",
)
@click.option(
    "--redis-port",
    "redis_port",
    type=click.INT,
    default=6379,
    help="Sets Redis port for caching results.",
)
@click.option(
    "--redis-db",
    "redis_db",
    type=click.INT,
    default=0,
    help="Sets Redis database for caching results.",
)
@click.option(
    "--redis-max-connection",
    "redis_max_connection",
    type=click.INT,
    default=20,
    help="Sets Redis max connection for caching results.",
)
def training_start(
    input_dir, detector, separator, lag, influence, n_standard_deviations, redis_host, redis_port, redis_db, redis_max_connection
):
    pipeline = DNSAnalyzerPipeline(
        path=input_dir,
        detector=detector,
        lag=lag,
        anomaly_influence=influence,
        n_standard_deviations=n_standard_deviations,
        separator=separator,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_max_connections=redis_max_connection
    )
    pipeline.run()


if __name__ == "__main__":
    """Default CLI entrypoint for Click interface
    """
    cli()
