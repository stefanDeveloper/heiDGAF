import logging

import click
import torch

from heidgaf import CONTEXT_SETTINGS
from heidgaf.main import Detector, DNSAnalyzerPipeline, FileType, Separator
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
    click.echo("Starting heiDGAF CLI")


@cli.command(name="check_gpu")
def check_gpu():
    # setting device on GPU if available, else CPU
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    click.echo(f"Using device: {device}")
    if torch.cuda.is_available():
        click.echo("GPU detected")
        click.echo(f"\t{torch.cuda.get_device_name(0)}")

    if device.type == "cuda":
        click.echo("Memory Usage:")
        click.echo(
            f"\tAllocated: {round(torch.cuda.memory_allocated(0)/1024**3,1)} GB"
        )
        click.echo(
            f"\tCached:    {round(torch.cuda.memory_reserved(0)/1024**3,1)} GB"
        )


@cli.command(name="train", context_settings={"show_default": True})
# @click.option(
#     "-m", 
#     "--model", 
#     "model", 
#     required=True, 
#     type=click.Path(), 
#     help="Input directory or file for analyzing."
# )
# @click.option(
#     "-d", 
#     "--dataset", 
#     "dataset", 
#     required=True, 
#     type=click.Path(), 
#     help="Input directory or file for analyzing."
# )
def training_model():
    click.echo("Start training of model.")
    trainer = DNSAnalyzerTraining(
        model=LogisticRegression(input_dim=9, output_dim=1, epochs=5000)
    )
    trainer.train()


@cli.command(name="process", context_settings={"show_default": True})
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
    type=click.Choice(Separator),
    default=Separator.SPACE,
    help="Separator type of input.",
)
@click.option(
    "--filetype",
    "filetype",
    type=click.Choice(FileType),
    default=FileType.TXT,
    help="File type of input.",
)
@click.option(
    "--lag",
    "lag",
    type=click.INT,
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
    input_dir, detector, separator, filetype, lag, influence, n_standard_deviations, redis_host, redis_port, redis_db, redis_max_connection
):
    click.echo("Starts processing log lines of DNS traffic.")
    pipeline = DNSAnalyzerPipeline(
        path=input_dir,
        detector=detector,
        lag=lag,
        anomaly_influence=influence,
        n_standard_deviations=n_standard_deviations,
        separator=separator,
        filetype=filetype,
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
