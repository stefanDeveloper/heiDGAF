import matplotlib.pyplot as plt

from benchmarking.input_data_handlers import CSVInputDataHandler
from benchmarking.plot_generators import ExactPlotGenerator

import pandas as pd

# # set start time
# ## set own start time or use the smallest time value
start_time = pd.Timestamp("2025-02-26 16:34:17.805245")
#
# # get data -> CLASS FOR HANDLING DATA
# ## load data from CSV, ClickHouse etc.
# ## put it in data structure to allow for multiple subplots in one figure
df = pd.read_csv("mock_data/collector.csv", parse_dates=["time"])
# ## do something with data
df = df.sort_values(by="time")  ## sort values by x axis
df["time"] = (
    df["time"] - start_time
).dt.total_seconds()  ## set values relative to start value


# data_handler = CSVInputDataHandler(input_data_file="mock_data/collector.csv")

generator = ExactPlotGenerator()
generator.set_up_figure()
generator.add_plot(
    x_data=df["time"],
    y_data=df["value"],
    y_scale=10**3,
    marker=None,
    line_style=None,
    label="test label",
    color_index=None,
)
generator.save("test_output.png")

#
# # set up the figure
# plot = plt.figure()
#
# # plot.plot(df["time"], df["value"] / (10 ** 3), marker=None, linestyle="-", label="label", color="test_color")
#
# plot.add_subplot(plt.plot(df["time"], df["value"] / (10 ** 3), marker=None, linestyle="-", label="label", color="test_color"))
import numpy as np

x = np.linspace(0, 2, 100)  # Sample data.

# Note that even in the OO-style, we use `.pyplot.figure` to create the Figure.
fig, ax = plt.subplots(layout="constrained")
fig.set_size_inches(10, 5)
ax.plot(x, x, label="linear")  # Plot some data on the Axes.
ax.plot(x, x**2, label="quadratic")  # Plot more data on the Axes...
ax.plot(x, x**3, label="cubic")  # ... and some more.
ax.set_xlabel("x label")  # Add an x-label to the Axes.
ax.set_ylabel("y label")  # Add a y-label to the Axes.
ax.set_title("Simple Plot")  # Add a title to the Axes.
ax.legend()  # Add a legend.

fig.savefig("test")
