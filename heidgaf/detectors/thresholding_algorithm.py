from typing import Dict, List, Tuple

import numpy as np

from heidgaf.detectors.base_anomaly import (AnomalyDetector,
                                            AnomalyDetectorConfig)


class ThresholdingAnomalyDetector(AnomalyDetector):
    """
    Anomaly detector based on thresholding technique using moving average and standard deviation filter.
    """

    def __init__(self, config: AnomalyDetectorConfig):
        super().__init__(config)

    def initialize_filters(
        self, y: List[float]
    ) -> Tuple[np.ndarray, List[float], List[float]]:
        signals, avg_filter, std_filter = self.create_arrays(y)
        avg_filter[self.lag - 1], std_filter[self.lag - 1] = self.moving_average_std(
            y[: self.lag], self.lag
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
            avg_filter, std_filter = self.update_avg_std(
                filtered_y, avg_filter, std_filter, i, self.lag
            )

        return self.prepare_output(signals, avg_filter, std_filter)

    def prepare_output(
        self, signals: List[float], avg_filter: List[float], std_filter: List[float]
    ):
        return dict(
            signals=np.asarray(AnomalyDetector.convert_signals_to_ints(signals)),
            avgFilter=np.asarray(avg_filter),
            stdFilter=np.asarray(std_filter),
        )
