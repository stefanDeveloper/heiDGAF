from typing import Optional

import pandas as pd
from matplotlib import pyplot as plt


class PlotGenerator:
    def plot_latency(
        self,
        datafiles_to_names: dict[str, str],
        title: str,
        start_time: Optional[pd.Timestamp] = None,
        x_label: str = "Time",
        y_label: str = "Latency",
        fig_width: int | float = 10,
        fig_height: int | float = 5,
        color_start_index: int = 0,
        intervals: Optional[list[int]] = None,
    ):
        plt.figure(figsize=(fig_width, fig_height))

        # initialize color palette
        colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]
        cur_color_index = color_start_index

        # load data from files
        dataframes = {}
        for file, label in datafiles_to_names.items():
            df = pd.read_csv(file, parse_dates=["time"]).sort_values(by="time")
            df["time"] = (df["time"] - start_time).dt.total_seconds()
            dataframes[label] = df

        x_unit = "s"

        y_scale = 10**3  # TODO: Calculate automatically
        y_unit = "ms"  # TODO: Calculate automatically

        # plot data
        for label, df in dataframes.items():
            plt.plot(
                df["time"],
                df["value"] / y_scale,
                marker=None,
                linestyle="-",
                label=label,
                color=colors[cur_color_index],
            )
            cur_color_index += 1

        # adjust settings
        plt.xlim(left=0)
        plt.ylim(bottom=0)
        # plt.gca().xaxis.set_major_locator(ticker.MultipleLocator(60))
        # plt.gca().yaxis.set_major_formatter(ticker.FormatStrFormatter("%.1f"))
        plt.title(title)
        plt.xlabel(f"Time [{x_unit}]")
        plt.ylabel(f"Latency [{y_unit}]")
        plt.grid(color="lightgray")

        if len(datafiles_to_names) > 1:
            plt.legend()

        plt.show()
