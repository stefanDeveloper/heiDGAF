from dataclasses import dataclass
from abc import ABC
from typing import List, Tuple, Dict, Union

import numpy as np
import polars as pl
import pandas as pd

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import plotly.graph_objects as go
from plotly.subplots import make_subplots


@dataclass
class AnomalyDetectorConfig:
    """Configuration for the AnomalyDetector."""
    lag: int
    threshold: float

class PolarsAnomalyDetector:
    """Anomaly Detector using the Polars library."""
    
    def __init__(self, config: AnomalyDetectorConfig):
        """Initialize with a configuration."""
        self.lag = config.lag
        self.threshold = config.threshold
    
    def calculate_rolling_mean(self, column: str) -> pl.Series:
        """Calculate rolling mean of a given column."""
        return pl.col(column).rolling_mean(self.lag, min_periods=2, center=False).alias(f"rolling_mean_n{self.lag}")

    def calculate_rolling_std(self, column: str) -> pl.Series:
        """Calculate rolling standard deviation of a given column."""
        return pl.col(column).rolling_std(self.lag, min_periods=2, center=False).alias(f"rolling_std_n{self.lag}")

    def calculate_y_subtract_rolling_mean(self, column: str, rolling_mean: pl.Series) -> pl.Series:
        """Subtract rolling mean from the column values."""
        return (pl.col(column) - rolling_mean).alias("y_subract_rolling_mean")

    def calculate_thresh_mult_by_std(self, rolling_std: pl.Series) -> pl.Series:
        """Calculate threshold multiplied by the rolling standard deviation."""
        return (rolling_std * self.threshold).alias("thresh_mult_by_std")

    def calculate_anomaly(self, column: str, rolling_mean: pl.Series, threshold_mult_by_std: pl.Series) -> pl.Series:
        """Detect anomalies based on the calculated threshold."""
        return (abs(pl.col(column) - rolling_mean) > threshold_mult_by_std).alias("anomaly")

    def calculate_signal(self, column: str, rolling_mean: pl.Series, threshold_mult_by_std: pl.Series) -> pl.Series:
        """Generate signals based on the detected anomalies."""
        y_subtract_rolling_mean = self.calculate_y_subtract_rolling_mean(column, rolling_mean)
        
        return (
            pl
            .when(y_subtract_rolling_mean > threshold_mult_by_std)
            .then(1)
            .when(y_subtract_rolling_mean < -threshold_mult_by_std)
            .then(-1)
            .otherwise(0)
            .alias("signal")
        )
    
    def run(self, df: Union[pl.DataFrame, pl.LazyFrame], column: str, mode: str = 'optimize') -> Union[pl.DataFrame, pl.LazyFrame]:
        """
        Run the anomaly detection process on the input DataFrame.
        """
        if not isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            raise ValueError("Input must be a Polars DataFrame or LazyFrame.")
        if not isinstance(column, str):
            raise ValueError("Column name must be a string.")
        if mode not in ['lazy', 'no_change', 'optimize']:
            raise ValueError("Mode must be one of 'lazy', 'no_change', 'optimize'.")

        if mode != 'no_change':
            df = df.lazy()

        rolling_mean = self.calculate_rolling_mean(column)
        rolling_std = self.calculate_rolling_std(column)
        threshold_mult_by_std = self.calculate_thresh_mult_by_std(rolling_std)

        df = df.with_columns([
            rolling_mean,
            rolling_std,
            threshold_mult_by_std,
            self.calculate_y_subtract_rolling_mean(column, rolling_mean),
            self.calculate_anomaly(column, rolling_mean, threshold_mult_by_std),
            self.calculate_signal(column, rolling_mean, threshold_mult_by_std)
        ])

        return df.collect() if mode == "optimize" else df

    
    def plot(self, df: pl.DataFrame, column: str, xlabel: str = 'Time', ylabel: str = 'Value', figsize: Tuple[int, int] = (12, 8)) -> plt.figure:
        fig = plt.figure(figsize=figsize, dpi=150)
        gs = gridspec.GridSpec(3, 1, height_ratios=[2, 0.5, 1])
        gs.update(wspace=1.5, hspace=0.025)
        ax1 = plt.subplot(gs[0])
        ax2 = plt.subplot(gs[1], sharex=ax1)
        axes = [ax1, ax2]
        

        signals = df['signal'].to_numpy()
        avg_filter = df[f"rolling_mean_n{self.lag}"]#.to_numpy()
        std_filter = df[f"rolling_std_n{self.lag}"]#.to_numpy()
        time_series = np.arange(df.shape[0])
        y = df[column].to_numpy()
        upper_bound = avg_filter + self.threshold * std_filter
        lower_bound = avg_filter - self.threshold * std_filter

        try:
            upper_bound = np.concatenate(upper_bound).ravel()
            lower_bound = np.concatenate(lower_bound).ravel()
        except ValueError:
            pass
        
        min_pos, max_pos = min(y.min(), lower_bound.min()), max(y.max(), upper_bound.max())
        
     
        ax1.plot(time_series, y, 'k.', label='Original Data', alpha=0.7)
        ax1.plot(time_series, avg_filter, ls='-', lw=2, c='steelblue', label='Moving Average')
        ax1.fill_between(time_series, lower_bound, upper_bound, color='lightsteelblue', alpha=0.3, label='Bounds')
        ax1.scatter(time_series[signals == 1], y[signals == 1], color='coral', s=20, zorder=5)
        ax1.scatter(time_series[signals == -1], y[signals == -1], color='coral', marker='o', s=20, zorder=5)
        
        for polarity in [1, -1]:
            ax1.vlines(time_series[signals == polarity], min_pos - (min_pos * .025), max_pos * 1.025, color="coral", alpha=0.2)
        
        ax2.plot(time_series, signals, ls='-', c='coral', label='Signals')

        for ax in axes:
            #ax.grid(True, which='major', c='gray', ls='-', lw=0.5, alpha=0.1)
            ax.tick_params(axis=u'both', which=u'both', length=0)
            for s in ["bottom", "top", "left", "right"]:
                ax.spines[s].set_visible(False)

        ax2.set_xlabel(xlabel, fontfamily="monospace")
        ax1.set_ylabel(ylabel, fontfamily="monospace")
        ax2.set_ylabel("Signal", fontfamily="monospace")

        ax1.set_xlim(time_series[0], time_series[-1])
        ax1.set_ylim(min_pos - (min_pos * .025), max_pos * 1.025) 
        ax2.set_xlim(time_series[0], time_series[-1])
        ax1.set_xticklabels([])
        plt.setp(ax2.get_yticklabels(), visible=False)

        return fig
    

    def interactive_plot(self, df: pl.DataFrame, column: str):
        signals = df['signal'].to_numpy()
        avg_filter = df[f"rolling_mean_n{self.lag}"]
        std_filter = df[f"rolling_std_n{self.lag}"]
        time_series = np.arange(df.shape[0])
        y = df[column].to_numpy()
        upper_bound = avg_filter + self.threshold * std_filter
        lower_bound = avg_filter - self.threshold * std_filter

        fig = make_subplots(
            rows=4,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.02,
            row_heights=[0.75, 0.25, 0.25, 0.25],  # specify the height ratio of each row
            specs=[
                [{"rowspan": 3}],
                [{}],
                [{}],
                [{}]
            ]
        )
        
        min_pos, max_pos = min(y.min(), lower_bound.min()), max(y.max(), upper_bound.max())
        
        for polarity in [1, -1]:
            indices = [i for i, x in enumerate(signals) if x == polarity]
            for i in indices:
                fig.add_shape(
                    type='line',
                    x0=time_series[i],
                    y0=min_pos,
                    x1=time_series[i],
                    y1=max_pos,
                    line=dict(
                        color='coral',
                        width=1,
                    ),opacity=0.5
                )

        # Upper and Lower Bounds
        fig.add_trace(
            go.Scatter(
                x=time_series,
                y=upper_bound,
                fill=None,
                mode='lines',
                name=None,
                line=dict(color='lightsteelblue', width=0),
                showlegend=False,
                hovertemplate="Upper Bound: %{y}"
            )
        )
        fig.add_trace(
            go.Scatter(
                x=time_series,
                y=lower_bound,
                fill='tonexty',
                mode='lines',
                name='Threshold',
                line=dict(color='lightsteelblue', width=0),
                hovertemplate="Lower Bound: %{y}"
            )
        )

        # Moving Average
        fig.add_trace(
            go.Scatter(
                x=time_series,
                y=avg_filter,
                mode='lines',
                name='Moving Average',
                line=dict(color='steelblue', width=3),
                hovertemplate="Moving Average: %{y}"
            )
        )

        # Original Data
        fig.add_trace(
            go.Scatter(
                x=time_series,
                y=y,
                mode='markers',
                name='Original Data',
                marker=dict(color='black', opacity=0.6, size=3),
                hovertemplate="Original Data: %{y}"
            )
        )

        # Signals
        fig.add_trace(
            go.Scatter(
                x=time_series[signals == 1],
                y=y[signals == 1],
                mode='markers',
                name='Anomaly',
                marker=dict(color='coral', size=6),
                hovertemplate="+ Anomaly: %{y}"
            )
        )
        fig.add_trace(
            go.Scatter(
                x=time_series[signals == -1],
                y=y[signals == -1],
                mode='markers',
                name=None,
                marker=dict(color='coral', size=6),
                showlegend=False,
                hovertemplate="- Anomaly: %{y}"
            )
        )
        
        # Signals plot
        fig.add_trace(
            go.Scatter(
                x=time_series,
                y=signals,
                mode="lines",
                name=None,
                marker=dict(color='coral'),
                showlegend=False,
                hovertemplate="Signal: %{y}"
            ),
            row=4,
            col=1
        )

        # Formatting
        fig.update_yaxes(title_text="Signals", tickvals=[1, 0, -1], row=4, col=1)
        fig.update_xaxes(title_text="Time", row=4, col=1)
        fig.update_layout(
            plot_bgcolor='white',
            showlegend=True,
            yaxis_title='Value',
            autosize=False,
            width=800,
            height=500,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=0.85
            ),
            font=dict(family="Monospace"),
        )
        
        fig.update_xaxes(showspikes=True, spikecolor="gray", spikesnap="cursor", spikemode="across", spikethickness=1)
        fig.update_yaxes(showspikes=True, spikecolor="gray", spikethickness=1)
        


        return fig
