import click
from heidgaf.train import train
from heidgaf import CONTEXT_SETTINGS
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
    click.secho("Starting heiDGAF CLI")

@cli.group(name="train", context_settings={"show_default": True})
def training_model():
    click.secho("Start training of model.")

@training_model.command(name="start")
def training_start():
    train()

if __name__ == "__main__":
    cli()