import click
import logging
from heidgaf.main import DNSAnalyzerPipeline
from heidgaf.train import train
from heidgaf import CONTEXT_SETTINGS
from heidgaf.version import __version__
import torch
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
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

    logging.info(f'Using device: {device}')
    if torch.cuda.is_available():
        logging.info("GPU detected")
        logging.info(f"\t{torch.cuda.get_device_name(0)}")
        
    if device.type == 'cuda':
        logging.info("Memory Usage:")
        logging.info(f"\tAllocated: {round(torch.cuda.memory_allocated(0)/1024**3,1)} GB")
        logging.info(f"\tCached:    {round(torch.cuda.memory_reserved(0)/1024**3,1)} GB")


@cli.group(name="train", context_settings={"show_default": True})
def training_model():
    logging.info("Start training of model.")


@training_model.command(name="start")
def training_start():
    train()


@cli.group(name="process", context_settings={"show_default": True})
def training_model():
    logging.info("Starts processing log lines of DNS traffic.")


@training_model.command(name="start")
@click.option("-r", "--read", "input_dir", required=True, type=click.Path())
def training_start(input_dir):
    pipeline = DNSAnalyzerPipeline(input_dir)
    pipeline.run()


if __name__ == "__main__":
    cli()
