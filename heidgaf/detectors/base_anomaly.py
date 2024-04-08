from abc import ABC
from dataclasses import dataclass
from typing import Dict, List, Tuple

import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import numpy as np


@dataclass
class AnomalyDetectorConfig:
    lag: int
    threshold: float
    influence: float


class AnomalyDetector(ABC):
    def __init__(self, config: AnomalyDetectorConfig):
        self.lag = config.lag
        self.threshold = config.threshold
        self.influence = config.influence

    @staticmethod
    def moving_average_std(data: List[float], window_size: int) -> Tuple[float, float]:
        return np.mean(data[-window_size:]), np.std(data[-window_size:])

    @staticmethod
    def update_filtered_series(
        data: float, influence: float, previous_filtered_value: float
    ) -> float:
        return influence * data + (1 - influence) * previous_filtered_value

    @staticmethod
    def update_avg_std(
        filtered_y: List[float],
        avg_filter: List[float],
        std_filter: List[float],
        i: int,
        lag: int,
    ) -> Tuple[List[float], List[float]]:
        avg_filter[i], std_filter[i] = AnomalyDetector.moving_average_std(
            filtered_y[i - lag + 1 : i + 1], lag
        )
        return avg_filter, std_filter

    @staticmethod
    def create_arrays(y: List[float]) -> Tuple[np.ndarray, List[float], List[float]]:
        return np.zeros(len(y)), [0] * len(y), [0] * len(y)

    @staticmethod
    def is_outlier(data: float, avg: float, std: float, threshold: float) -> bool:
        return abs(data - avg) > threshold * std

    @staticmethod
    def convert_signals_to_ints(signals: List[float]) -> List[int]:
        return [int(signal) for signal in signals]

    def plot(
        self,
        y: List[float],
        results: Dict[str, np.ndarray],
        xlabel: str = "Time",
        ylabel: str = "Value",
        figsize: Tuple[int, int] = (12, 8),
    ) -> plt.figure:
        fig = plt.figure(figsize=figsize, dpi=150)
        gs = gridspec.GridSpec(3, 1, height_ratios=[2, 0.5, 1])
        gs.update(wspace=1.5, hspace=0.025)
        ax1 = plt.subplot(gs[0])
        ax2 = plt.subplot(gs[1], sharex=ax1)
        axes = [ax1, ax2]

        signals = results["signals"][self.lag :]
        avg_filter = results["avgFilter"][self.lag :]
        std_filter = results["stdFilter"][self.lag :]
        time_series = np.arange(len(y))[self.lag :]
        y = y[self.lag :]
        upper_bound = avg_filter + self.threshold * std_filter
        lower_bound = avg_filter - self.threshold * std_filter

        try:
            upper_bound = np.concatenate(upper_bound).ravel()
            lower_bound = np.concatenate(lower_bound).ravel()
        except ValueError:
            pass

        min_pos, max_pos = min(y.min(), lower_bound.min()), max(
            y.max(), upper_bound.max()
        )

        ax1.plot(time_series, y, "k.", label="Original Data", alpha=0.7)
        ax1.plot(
            time_series, avg_filter, ls="-", lw=2, c="steelblue", label="Moving Average"
        )
        ax1.fill_between(
            time_series,
            lower_bound,
            upper_bound,
            color="lightsteelblue",
            alpha=0.3,
            label="Bounds",
        )
        ax1.scatter(
            time_series[signals == 1], y[signals == 1], color="coral", s=20, zorder=5
        )
        ax1.scatter(
            time_series[signals == -1],
            y[signals == -1],
            color="coral",
            marker="o",
            s=20,
            zorder=5,
        )

        for polarity in [1, -1]:
            ax1.vlines(
                time_series[signals == polarity],
                min_pos,
                max_pos,
                color="coral",
                alpha=0.2,
            )

        ax2.plot(time_series, signals, ls="-", c="coral", label="Signals")

        for ax in axes:
            ax.grid(True, which="major", c="gray", ls="-", lw=0.5, alpha=0.1)
            ax.tick_params(axis="both", which="both", length=0)
            for s in ["bottom", "top", "left", "right"]:
                ax.spines[s].set_visible(False)

        ax2.set_xlabel(xlabel, fontfamily="monospace")
        ax1.set_ylabel(ylabel, fontfamily="monospace")
        ax2.set_ylabel("Signal", fontfamily="monospace")

        ax1.set_xlim(time_series[0], time_series[-1])
        ax1.set_ylim(min_pos, max_pos)
        ax2.set_xlim(time_series[0], time_series[-1])
        ax1.set_xticklabels([])
        plt.setp(ax2.get_yticklabels(), visible=False)

        return fig
