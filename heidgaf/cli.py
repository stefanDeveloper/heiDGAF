import click

from heidgaf import CONTEXT_SETTINGS
from heidgaf.main import Detector, DNSInspectorPipeline, FileType, Separator
from heidgaf.train import Dataset, DNSAnalyzerTraining, Model
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

@cli.command(name="train", context_settings={"show_default": True})
@click.option(
    "-m", 
    "--model", 
    "model", 
    required=True, 
    type=click.Choice(Model),
    help="Model for fitting."
)
@click.option(
    "-d", 
    "--dataset", 
    "dataset", 
    required=True, 
    type=click.Choice(Dataset),
    default=Dataset.ALL,
    help="Dataset for fitting."
)
@click.option(
    "-o", 
    "--output_dir", 
    "output_dir", 
    required=True, 
    type=click.STRING,
    help="Output path of model."
)
def train(model, dataset, output_dir):
    click.echo("Start training of model.")
    trainer = DNSAnalyzerTraining(
        model=model,
        dataset=dataset
    )
    trainer.train(output_path=output_dir)


@cli.command(name="inspect", context_settings={"show_default": True})
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
    "-m", 
    "--model", 
    "model", 
    required=True, 
    type=click.Choice(Model),
    help="Model for prediction."
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
def inspection(
    input_dir, detector, model, separator, filetype, lag, influence, n_standard_deviations, redis_host, redis_port, redis_db, redis_max_connection
):
    click.echo("Starts processing log lines of DNS traffic.")
    pipeline = DNSInspectorPipeline(
        path=input_dir,
        detector=detector,
        model=model,
        lag=lag,
        anomaly_influence=influence,
        n_standard_deviations=n_standard_deviations,
        separator=separator,
        filetype=filetype,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_max_connections=redis_max_connection,
    )
    pipeline.run()


if __name__ == "__main__":
    """Default CLI entrypoint for Click interface
    """
    cli()
