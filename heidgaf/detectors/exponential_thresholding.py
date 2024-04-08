from typing import Dict, List, Tuple

import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import numpy as np

from .base_anomaly import AnomalyDetector, AnomalyDetectorConfig


class EMAAnomalyDetector(AnomalyDetector):
    """
    Anomaly detector based on thresholding technique using Exponential Moving Average (EMA) and standard deviation filter.
    """

    def __init__(self, config: AnomalyDetectorConfig, alpha: float = 0.1):
        super().__init__(config)
        self.alpha = alpha

    @staticmethod
    def exponential_moving_average_std(
        data: List[float], alpha: float
    ) -> Tuple[float, float]:
        ema = data[0]
        for value in data[1:]:
            ema = alpha * value + (1 - alpha) * ema
        std = np.std(data)
        return ema, std

    def initialize_filters(
        self, y: List[float]
    ) -> Tuple[np.ndarray, List[float], List[float]]:
        signals, avg_filter, std_filter = self.create_arrays(y)
        avg_filter[self.lag - 1], std_filter[self.lag - 1] = (
            self.exponential_moving_average_std(y[: self.lag], self.alpha)
        )
        return signals, avg_filter, std_filter

    def update_signal_and_filtered_series(
        self,
        data: float,
        i: int,
        avg_filter: List[float],
        std_filter: List[float],
        signals: np.ndarray,
        filtered_y: np.ndarray,
    ):
        is_outlier = self.is_outlier(
            data, avg_filter[i - 1], std_filter[i - 1], self.threshold
        )
        signals[i] = np.sign(data - avg_filter[i - 1]) * is_outlier
        filtered_y[i] = (
            self.update_filtered_series(data, self.influence, filtered_y[i - 1])
            if is_outlier
            else data
        )
        return signals, filtered_y

    def run(self, y: List[float]) -> Dict[str, np.ndarray]:
        y = np.asarray(y)
        filtered_y = np.array(y)
        signals, avg_filter, std_filter = self.initialize_filters(y)

        for i, data in enumerate(y[self.lag :], start=self.lag):
            signals, filtered_y = self.update_signal_and_filtered_series(
                data, i, avg_filter, std_filter, signals, filtered_y
            )
            avg_filter[i], std_filter[i] = self.exponential_moving_average_std(
                filtered_y[i - self.lag + 1 : i + 1], self.alpha
            )

        return self.prepare_output(signals, avg_filter, std_filter)

    def prepare_output(self, signals, avg_filter, std_filter):
        return dict(
            signals=np.asarray(signals),
            avgFilter=np.asarray(avg_filter),
            stdFilter=np.asarray(std_filter),
        )
