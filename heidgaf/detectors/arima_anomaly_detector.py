from typing import Dict, List, Tuple

import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import numpy as np
from statsmodels.tsa.arima.model import ARIMA

from heidgaf.detectors.base_anomaly import (AnomalyDetector,
                                            AnomalyDetectorConfig)


class ARIMAAnomalyDetector(AnomalyDetector):
    """
    Anomaly detector based on the ARIMA model.
    """

    def __init__(self, config: AnomalyDetectorConfig, order: Tuple[int, int, int]):
        super().__init__(config)
        self.order = order
        self.model = None
        self.model_fit = None
        self.y = None

    def fit_model(self, y: List[float]):
        self.y = np.asarray(y)
        self.model = ARIMA(self.y, order=self.order)
        self.model_fit = self.model.fit()

    def get_predictions(self):
        return self.model_fit.predict(start=1, end=len(self.y))

    def get_residuals(self, predictions):
        return self.y - predictions

    def get_std_residual(self, residuals):
        return np.std(residuals)

    def get_anomalies(self, residuals, std_residual):
        return np.abs(residuals) > self.threshold * std_residual

    def run(self, y: List[float]) -> Dict[str, np.ndarray]:
        self.fit_model(y)
        predictions = self.get_predictions()
        residuals = self.get_residuals(predictions)
        std_residual = self.get_std_residual(residuals)
        is_anomaly = self.get_anomalies(residuals, std_residual)
        return dict(predictions=predictions, residuals=residuals, anomalies=is_anomaly)

    @staticmethod
    def rmse_metric(residuals):
        return np.sqrt(np.mean((residuals) ** 2))

    def plot(
        self,
        y: List[float],
        results: Dict[str, np.ndarray],
        xlabel: str = "Time",
        ylabel: str = "Value",
        figsize: Tuple[int, int] = (12, 8),
    ) -> plt.figure:
        fig = plt.figure(figsize=figsize, dpi=150)
        gs = gridspec.GridSpec(3, 1, height_ratios=[2, 0.5, 0.5])
        gs.update(wspace=1.5, hspace=0.025)
        ax1 = plt.subplot(gs[0])
        ax2 = plt.subplot(gs[1], sharex=ax1)
        ax3 = plt.subplot(gs[2], sharex=ax1)
        axes = [ax1, ax2, ax3]

        time_series = np.arange(len(y))
        residuals = results["residuals"]
        predictions = results["predictions"]
        anomalies = results["anomalies"]

        std_residual = np.std(residuals)
        upper_bound = predictions + self.threshold * std_residual
        lower_bound = predictions - self.threshold * std_residual

        min_pos, max_pos = min(y.min(), predictions.min(), lower_bound.min()), max(
            y.max(), predictions.max(), upper_bound.max()
        )

        # predictions vs actual
        ax1.fill_between(
            time_series,
            lower_bound,
            upper_bound,
            color="lightsteelblue",
            alpha=0.3,
            label="Bounds",
        )
        ax1.plot(time_series, y, "k.", label="Original Data", alpha=0.7)
        ax1.plot(
            time_series,
            predictions,
            ls="-",
            lw=2,
            c="steelblue",
            label="ARIMA Predictions",
        )
        ax1.scatter(
            time_series[anomalies == 1],
            y[anomalies == 1],
            color="coral",
            s=20,
            zorder=5,
        )
        ax1.vlines(
            time_series[anomalies == 1], min_pos, max_pos, color="coral", alpha=0.2
        )

        # residuals
        ax2.plot(time_series, residuals, "k.", label="Residuals", alpha=0.7)
        ax2.scatter(
            time_series[anomalies], residuals[anomalies], color="coral", s=20, zorder=5
        )
        ax2.vlines(
            time_series[anomalies],
            residuals.min(),
            residuals.max(),
            color="coral",
            zorder=5,
            alpha=0.2,
        )

        # signals
        ax3.plot(time_series, anomalies, ls="-", c="coral", label="Anomalies")

        # labels
        ax1.set_ylabel(ylabel, fontfamily="monospace")
        ax1.set_xlim(time_series[0], time_series[-1])
        ax1.set_xticklabels([])
        ax1.set_ylim(min_pos, max_pos)
        ax1.legend()

        ax2.set_ylabel("Residuals", fontfamily="monospace")
        ax2.set_xlim(time_series[0], time_series[-1])

        ax3.set_xticklabels([])
        ax3.set_xlabel(xlabel, fontfamily="monospace")
        ax3.set_ylabel("Signal", fontfamily="monospace")
        plt.setp(ax3.get_yticklabels(), visible=False)
        # format
        for ax in axes:
            ax.grid(True, which="major", c="gray", ls="-", lw=0.5, alpha=0.1)
            ax.tick_params(axis="both", which="both", length=0)
            for s in ["bottom", "top", "left", "right"]:
                ax.spines[s].set_visible(False)

        return fig
