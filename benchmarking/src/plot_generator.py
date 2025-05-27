import datetime
from typing import Optional

import pandas as pd
from matplotlib import pyplot as plt, ticker


class PlotGenerator:
    """Plots given data and combines it into figures."""

    def plot_latency(
        self,
        datafiles_to_names: dict[str, str],
        title: str,
        start_time: pd.Timestamp,
        median_smooth: bool = False,
        destination_file: str = "../graphs/",
        x_label: str = "Time",
        y_label: str = "Latency",
        y_input_unit: str = "microseconds",
        fig_width: int | float = 10,
        fig_height: int | float = 5,
        color_start_index: int = 0,
        intervals_in_sec: Optional[list[int]] = None,
    ):
        """Creates a figure and plots the given latency data as graphs. All graphs are plotted into the same figure,
        which is then stored as a file.

        Args:
            datafiles_to_names (dict[str, str]): Dictionary of file paths and their names to show in the legend
            title (str): Title of the figure
            destination_file (str): File path at which the figure should be stored
            start_time (pd.Timestamp): Time to be set as t = 0
            median_smooth (bool): True if the data should be smoothed, False by default
            x_label (str): Label x-axis, "Time" by default
            y_label (str): Label y-axis, "Latency" by default
            y_input_unit (str): Unit of the data given as input, "microseconds" by default
            fig_width (int | float): Width of the figure, 10 by default
            fig_height (int | float): Height of the figure, 5 by default
            color_start_index (int): First index of the color palette to be used, 0 by default
            intervals_in_sec (Optional[list[int]]): Optional list of interval lengths in seconds
        """
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

            if median_smooth:
                window_size = max(1, len(df) // 100)
                df["value"] = (
                    df["value"]
                    .rolling(window=window_size, center=True, min_periods=1)
                    .median()
                )

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
                df["time"] / x_scale * (10**6),
                df["value"] / y_scale,
                marker=None,
                linestyle="-",
                label=label,
                color=colors[cur_color_index],
            )
            cur_color_index += 1

        # add interval lines
        if intervals_in_sec is not None:
            x_values = [0]
            for i in intervals_in_sec:
                x_values.append(x_values[-1] + i)
            for x in x_values[1:]:
                plt.axvline(x / 60, color="gray", linestyle="--", linewidth=1)

        # adjust settings
        plt.xlim(left=0)
        plt.ylim(bottom=0)

        if x_unit == "s":
            plt.gca().xaxis.set_major_locator(ticker.MultipleLocator(30))

        y_label_additions = ""
        if median_smooth:
            y_label_additions = " (median-smoothed)"

        plt.title(title)
        plt.xlabel(f"{x_label} [{x_unit}]")
        plt.ylabel(f"{y_label} [{y_unit}]" + y_label_additions)
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
            ("ms", 10**3),
            ("s", 10**6),
            ("min", 60 * (10**6)),
            ("h", 60 * 60 * (10**6)),
            ("d", 24 * 60 * 60 * (10**6)),
        ]
        thresholds = [
            1.3 * (10**3),  # ms for over 1.3 ms
            1.3 * (10**6),  # s for over 1.3 s
            5 * 60 * (10**6),  # min for over 5 min
            3 * 60 * 60 * (10**6),  # h for over 3 h
            3 * 24 * 60 * 60 * (10**6),  # d for over 3 d
            float("inf"),  # d for everything above
        ]

        for (unit, factor), threshold in zip(units, thresholds):
            if max_value_in_microseconds < threshold:
                return unit, factor
