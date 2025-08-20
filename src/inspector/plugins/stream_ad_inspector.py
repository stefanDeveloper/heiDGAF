from src.inspector.inspector import InspectorBase
import importlib
import os
import sys
from datetime import datetime
import numpy as np
from streamad.util import StreamGenerator, CustomDS
# TODO: test all of this!
sys.path.append(os.getcwd())
from src.base.utils import (
    setup_config
)
from src.base.log_config import get_logger

module_name = "data_inspection.inspector"
logger = get_logger(module_name)

config = setup_config()
VALID_UNIVARIATE_MODELS = [
    "KNNDetector",
    "SpotDetector",
    "SRDetector",
    "ZScoreDetector",
    "OCSVMDetector",
    "MadDetector",
    "SArimaDetector",
]

VALID_MULTIVARIATE_MODELS = [
    "xStreamDetector",
    "RShashDetector",
    "HSTreeDetector",
    "LodaDetector",
    "OCSVMDetector",
    "RrcfDetector",
]

VALID_ENSEMBLE_MODELS = ["WeightEnsemble", "VoteEnsemble"]

STATIC_ZEROS_UNIVARIATE = np.zeros((100, 1))
STATIC_ZEROS_MULTIVARIATE = np.zeros((100, 2))


class StreamADInspector(InspectorBase):

    def __init__(self, consume_topic, produce_topics, config):
        super().__init__(consume_topic, produce_topics, config)
        self.ensemble_config = config["ensemble"]

    def subnet_is_suspicious(self) -> bool: 
        total_anomalies = np.count_nonzero(
            np.greater_equal(np.array(self.anomalies), self.score_threshold)
        )
        logger.info(f"{self.name}: {total_anomalies} anomalies found")
        return True if total_anomalies / len(self.X) > self.anomaly_threshold else False
    
    def _mean_packet_size(self, messages: list, begin_timestamp, end_timestamp):
        """Returns mean of packet size of messages between two timestamps given a time step.
        By default, 1 ms time step is applied. Time steps are adjustable by "time_type" and "time_range"
        in config.yaml.

        Args:
            messages (list): Messages from KafkaConsumeHandler.
            begin_timestamp (datetime): Begin timestamp of batch.
            end_timestamp (datetime): End timestamp of batch.

        Returns:
            numpy.ndarray: 2-D numpy.ndarray including all steps.
        """
        logger.debug("Convert timestamps to numpy datetime64")
        timestamps = np.array(
            [np.datetime64(datetime.fromisoformat(item["ts"])) for item in messages]
        )

        # Extract and convert the size values from "111b" to integers
        sizes = np.array([int(str(item["size"]).replace("b", "")) for item in messages])

        logger.debug("Sort timestamps and count occurrences")
        sorted_indices = np.argsort(timestamps)
        timestamps = timestamps[sorted_indices]
        sizes = sizes[sorted_indices]

        logger.debug("Set min_date and max_date")
        min_date = np.datetime64(begin_timestamp)
        max_date = np.datetime64(end_timestamp)

        logger.debug(
            "Generate the time range from min_date to max_date with 1ms interval"
        )
        time_range = np.arange(
            min_date,
            max_date + np.timedelta64(self.time_range, self.time_type),
            np.timedelta64(self.time_range, self.time_type),
        )

        logger.debug(
            "Initialize an array to hold counts for each timestamp in the range"
        )
        counts = np.zeros(time_range.shape, dtype=np.float64)
        size_sums = np.zeros(time_range.shape, dtype=np.float64)
        mean_sizes = np.zeros(time_range.shape, dtype=np.float64)

        # Handle empty messages.
        if len(messages) > 0:
            logger.debug(
                "Count occurrences of timestamps and fill the corresponding index in the counts array"
            )
            _, unique_indices, unique_counts = np.unique(
                timestamps, return_index=True, return_counts=True
            )

            # Sum the sizes at each unique timestamp
            for idx, count in zip(unique_indices, unique_counts):
                time_index = (
                    ((timestamps[idx] - min_date) // self.time_range)
                    .astype(f"timedelta64[{self.time_type}]")
                    .astype(int)
                )
                size_sums[time_index] = np.sum(sizes[idx : idx + count])
                counts[time_index] = count

            # Calculate the mean packet size for each millisecond (ignore division by zero warnings)
            with np.errstate(divide="ignore", invalid="ignore"):
                mean_sizes = np.divide(
                    size_sums, counts, out=np.zeros_like(size_sums), where=counts != 0
                )
        else:
            logger.warning("Empty messages to inspect.")

        logger.debug("Reshape into the required shape (n, 1)")
        return mean_sizes.reshape(-1, 1)

    def _count_errors(self, messages: list, begin_timestamp, end_timestamp):
        """Counts occurances of messages between two timestamps given a time step.
        By default, 1 ms time step is applied. Time steps are adjustable by "time_type" and "time_range"
        in config.yaml.

        Args:
            messages (list): Messages from KafkaConsumeHandler.
            begin_timestamp (datetime): Begin timestamp of batch.
            end_timestamp (datetime): End timestamp of batch.

        Returns:
            numpy.ndarray: 2-D numpy.ndarray including all steps.
        """
        logger.debug("Convert timestamps to numpy datetime64")
        timestamps = np.array(
            [np.datetime64(datetime.fromisoformat(item["ts"])) for item in messages]
        )

        logger.debug("Sort timestamps and count occurrences")
        sorted_indices = np.argsort(timestamps)
        timestamps = timestamps[sorted_indices]

        logger.debug("Set min_date and max_date")
        min_date = np.datetime64(begin_timestamp)
        max_date = np.datetime64(end_timestamp)

        logger.debug(
            "Generate the time range from min_date to max_date with 1ms interval"
        )
        # Adding np.timedelta adds end time to time_range
        time_range = np.arange(
            min_date,
            max_date + np.timedelta64(self.time_range, self.time_type),
            np.timedelta64(self.time_range, self.time_type),
        )

        logger.debug(
            "Initialize an array to hold counts for each timestamp in the range"
        )
        counts = np.zeros(time_range.shape, dtype=np.float64)

        # Handle empty messages.
        if len(messages) > 0:
            logger.debug(
                "Count occurrences of timestamps and fill the corresponding index in the counts array"
            )
            unique_times, _, unique_counts = np.unique(
                timestamps, return_index=True, return_counts=True
            )
            time_indices = (
                ((unique_times - min_date) // self.time_range)
                .astype(f"timedelta64[{self.time_type}]")
                .astype(int)
            )
            counts[time_indices] = unique_counts
        else:
            logger.warning("Empty messages to inspect.")

        logger.debug("Reshape into the required shape (n, 1)")
        return counts.reshape(-1, 1)
    
    def _get_models(self, models):
        if hasattr(self, "models") and self.models != None and self.models != []:
            logger.info("All models have been successfully loaded!")
            return

        model_list = []
        for model in models:
            if self.mode == "univariate" or self.mode == "ensemble":
                logger.debug(f"Load Model: {model['model']} from {model['module']}.")
                if not model["model"] in VALID_UNIVARIATE_MODELS:
                    logger.error(
                        f"Model {models} is not a valid univariate or ensemble model."
                    )
                    raise NotImplementedError(
                        f"Model {models} is not a valid univariate or ensemble model."
                    )
            if self.mode == "multivariate":
                logger.debug(f"Load Model: {model['model']} from {model['module']}.")
                if not model["model"] in VALID_MULTIVARIATE_MODELS:
                    logger.error(f"Model {model} is not a valid multivariate model.")
                    raise NotImplementedError(
                        f"Model {model} is not a valid multivariate model."
                    )

            module = importlib.import_module(model["module"])
            module_model = getattr(module, model["model"])
            model_list.append(module_model(**model["model_args"]))
        return model_list
    def inspect_anomalies(self):
        match self.mode:
            case "univariate":
                self._inspect_univariate()
            case "multivariate":
                self._inspect_multivariate()
            case "ensemble":
                self._get_ensemble()
                self._inspect_ensemble()
            case _:
                logger.warning(f"Mode {self.mode} is not supported!")
                raise NotImplementedError(f"Mode {self.mode} is not supported!")     

    def _inspect_multivariate(self):
        """
        Method to inspect multivariate data for anomalies using a StreamAD Model
        Errors are count in the time window and fit model to retrieve scores.

        Args:
            model (str): Model name (should be capable of handling multivariate data)

        """

        logger.debug("Inspecting data...")

        X_1 = self._mean_packet_size(
            self.messages, self.begin_timestamp, self.end_timestamp
        )
        X_2 = self._count_errors(
            self.messages, self.begin_timestamp, self.end_timestamp
        )

        self.X = np.concatenate((X_1, X_2), axis=1)

        # TODO Append zeros to avoid issues when model is reused.
        # self.X = np.vstack((STATIC_ZEROS_MULTIVARIATE, X))

        ds = CustomDS(self.X, self.X)
        stream = StreamGenerator(ds.data)

        for x in stream.iter_item():
            score = self.models[0].fit_score(x)
            # noqa
            if score != None:
                self.anomalies.append(score)
            else:
                self.anomalies.append(0)

    def _inspect_ensemble(self):
        """
        Method to inspect data for anomalies using ensembles of two StreamAD models
        Errors are count in the time window and fit model to retrieve scores.
        """
        self.X = self._count_errors(
            self.messages, self.begin_timestamp, self.end_timestamp
        )

        # TODO Append zeros to avoid issues when model is reused.
        # self.X = np.vstack((STATIC_ZEROS_UNIVARIATE, X))

        ds = CustomDS(self.X, self.X)
        stream = StreamGenerator(ds.data)

        for x in stream.iter_item():
            scores = []
            # Fit all models in ensemble
            for model in self.models:
                scores.append(model.fit_score(x))
            # TODO Calibrators are missing
            score = self.ensemble.ensemble(scores)
            # noqa
            if score != None:
                self.anomalies.append(score)
            else:
                self.anomalies.append(0)

    def _inspect_univariate(self):
        """Runs anomaly detection on given StreamAD Model on univariate data.
        Errors are count in the time window and fit model to retrieve scores.

        Args:
            model (str): StreamAD model name.
        """

        logger.debug("Inspecting data...")

        self.X = self._count_errors(
            self.messages, self.begin_timestamp, self.end_timestamp
        )

        # TODO Append zeros to avoid issues when model is reused.
        # self.X = np.vstack((STATIC_ZEROS_UNIVARIATE, X))

        ds = CustomDS(self.X, self.X)
        stream = StreamGenerator(ds.data)

        for x in stream.iter_item():
            score = self.models[0].fit_score(x)
            # noqa
            if score is not None:
                self.anomalies.append(score)
            else:
                self.anomalies.append(0)



    def _get_ensemble(self):
        logger.debug(
            f"Load Model: {self.ensemble_config['model']} from {self.ensemble_config['module']}."
        )
        if not self.ensemble_config["model"] in VALID_ENSEMBLE_MODELS:
            logger.error(f"Model {self.ensemble_config} is not a valid ensemble model.")
            raise NotImplementedError(
                f"Model {self.ensemble_config} is not a valid ensemble model."
            )

        if hasattr(self, "ensemble") and self.ensemble != None:
            logger.info("Ensemble have been successfully loaded!")
            return

        module = importlib.import_module(self.ensemble_config["module"])
        module_model = getattr(module, self.ensemble_config["model"])
        self.ensemble = module_model(**self.ensemble_config["model_args"])
