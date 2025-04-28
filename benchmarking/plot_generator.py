import datetime
from typing import Optional

import pandas as pd
from matplotlib import pyplot as plt


class PlotGenerator:
    def plot_latency(
        self,
        datafiles_to_names: dict[str, str],
        title: str,
        destination_file: str,
        start_time: Optional[pd.Timestamp] = None,
        x_label: str = "Time",
        y_label: str = "Latency",
        y_input_unit: str = "microseconds",
        fig_width: int | float = 10,
        fig_height: int | float = 5,
        color_start_index: int = 0,
        intervals: Optional[list[int]] = None,
    ):
        """Creates a figure and plots the given latency data as graphs. All graphs are plotted into the same figure,
        which is then stored as a file."""
        plt.figure(figsize=(fig_width, fig_height))

        # initialize color palette
        colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]
        cur_color_index = color_start_index

        # load data from files
        dataframes = {}
        total_max_time = 0  # seconds
        total_max_value = 0

        for file, label in datafiles_to_names.items():
            df = pd.read_csv(file, parse_dates=["time"]).sort_values(by="time")
            df["time"] = (df["time"] - start_time).dt.total_seconds()
            dataframes[label] = df

            df_max_time = df["time"].max()
            if df_max_time > total_max_time:
                total_max_time = df_max_time

            df_max_value = df["value"].max()
            if df_max_value > total_max_value:
                total_max_value = df_max_value

        x_unit, x_scale = self._determine_time_unit(total_max_time, "seconds")
        y_unit, y_scale = self._determine_time_unit(total_max_value, y_input_unit)

        # plot data
        for label, df in dataframes.items():
            plt.plot(
                df["time"] / x_scale * 10**6,
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
        plt.xlabel(f"{x_label} [{x_unit}]")
        plt.ylabel(f"{y_label} [{y_unit}]")
        plt.grid(color="lightgray")

        if len(datafiles_to_names) > 1:
            plt.legend()

        plt.savefig(destination_file, dpi=300, bbox_inches="tight")

    @staticmethod
    def _determine_time_unit(max_value: int, input_unit: str):
        max_value_in_seconds = datetime.timedelta(
            **{input_unit: int(max_value)}
        ).total_seconds()
        max_value_in_microseconds = max_value_in_seconds * (10**6)

        units = [
            ("us", 1),
            ("ms", 1_000),
            ("s", 1_000_000),
            ("min", 60_000_000),
            ("h", 3_600_000_000),
            ("d", 86_400_000_000),
        ]
        thresholds = [
            1_400,  # ms for over 1.4 ms
            1_400_000,  # s for over 1.4 s
            300_000_000,  # min for over 5 min
            5_000_000_000,  # h for over 5 min
            112_320_000_000,  # d for over 1.3 d
            float("inf"),  # d for everything above
        ]

        for (unit, factor), threshold in zip(units, thresholds):
            if max_value_in_microseconds < threshold:
                return unit, factor
