from abc import abstractmethod
from typing import Optional, Any

from matplotlib import pyplot as plt


class PlotGenerator:
    """Base class for generators that plot data from a data input."""

    def __init__(self):
        self.figure = None
        self.axes = None

    def set_up_figure(self):
        """Sets up the figure and axes."""
        self.figure, self.axes = plt.subplots(layout="constrained")
        self.figure.set_size_inches(10, 5)  # default size

    def configure(
        self,
        size: Optional[tuple[float | int, float | int]],
        x_label: Optional[str],
        y_label: Optional[str],
        title: Optional[str],
        legend: Optional[bool],
    ):
        if size:
            self.figure.set_size_inches(size[0], size[1])

        if x_label:
            self.axes.set_xlabel(x_label)

        if y_label:
            self.axes.set_ylabel(y_label)

        if title:
            self.axes.set_title(title)

        if legend:
            self.axes.legend()

    def save(self, output_filename: str):
        """Saves the figure in a file with the given name.

        Args:
            output_filename (str): Name of the file in which to store the figure
        """
        self.figure.savefig(output_filename)

    @abstractmethod
    def add_plot(self, *args, **kwargs):
        """Adds a plot with the given parameters as configuration."""
        raise NotImplementedError


class ExactPlotGenerator(PlotGenerator):
    """Generates exact plots using the input data."""

    def add_plot(
        self,
        x_data: Any,
        y_data: Any,
        y_scale: float | int,
        marker: Optional[str],
        line_style: Optional[str],
        label: str,
        color_index: Optional[int],
    ):
        self.axes.plot(
            x_data, y_data / y_scale, marker=marker, linestyle=line_style, label=label
        )
