from typing import List

import numpy as np

from heidgaf.detectors.base_anomaly import (AnomalyDetector,
                                            AnomalyDetectorConfig)


class RealTimeAnomalyDetector(AnomalyDetector):
    """
    Anomaly detector for real-time data streams based on moving average and standard deviation filter.
    """

    def __init__(self, config: AnomalyDetectorConfig):
        super().__init__(config)
        self.signals: List[int] = []
        self.filtered_y: List[float] = []
        self.avg_filter: List[float] = []
        self.std_filter: List[float] = []

    def append_data(self, data: float):
        self.filtered_y.append(data)

    def initialize_filters(self):
        if len(self.filtered_y) == self.lag:
            avg, std = self.moving_average_std(self.filtered_y[-self.lag :], self.lag)
            self.avg_filter.append(avg)
            self.std_filter.append(std)

    def calculate_signal(self, data: float):
        is_outlier = self.is_outlier(
            data, self.avg_filter[-1], self.std_filter[-1], self.threshold
        )
        signal = np.sign(data - self.avg_filter[-1]) * is_outlier
        self.signals.append(signal)
        return is_outlier

    def update_filters(self, data: float, is_outlier: bool):
        if is_outlier:
            filtered_data = self.update_filtered_series(
                data, self.influence, self.filtered_y[-2]
            )
            self.filtered_y[-1] = filtered_data
            avg, std = self.moving_average_std(self.filtered_y[-self.lag :], self.lag)
            self.avg_filter.append(avg)
            self.std_filter.append(std)
        else:
            self.avg_filter.append(self.avg_filter[-1])
            self.std_filter.append(self.std_filter[-1])

    def run(self, data: float) -> int:
        self.append_data(data)

        if len(self.filtered_y) < self.lag:
            self.signals.append(0)
        else:
            self.initialize_filters()
            is_outlier = self.calculate_signal(data)
            self.update_filters(data, is_outlier)
        return int(self.signals[-1])
