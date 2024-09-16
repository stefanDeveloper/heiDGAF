import ast
from datetime import datetime
from enum import Enum, unique
import json
import logging
import os
import sys
import importlib
import numpy as np

from streamad.util import StreamGenerator, CustomDS

sys.path.append(os.getcwd())
from src.base.utils import setup_config
from src.base.kafka_handler import (
    KafkaConsumeHandler,
    KafkaMessageFetchException,
    KafkaProduceHandler,
)
from src.base.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

config = setup_config()
MODE = config["heidgaf"]["inspector"]["mode"]
ENSEMBLE = config["heidgaf"]["inspector"]["ensemble"]
MODELS = config["heidgaf"]["inspector"]["models"]
ANOMALY_THRESHOLD = config["heidgaf"]["inspector"]["anomaly_threshold"]
SCORE_THRESHOLD = config["heidgaf"]["inspector"]["score_threshold"]
TIME_TYPE = config["heidgaf"]["inspector"]["time_type"]
TIME_RANGE = config["heidgaf"]["inspector"]["time_range"]
TIMESTAMP_FORMAT = config["heidgaf"]["timestamp_format"]


VALID_UNIVARIATE_MODELS = [
    "KNNDetector",
    "SpotDetector",
    "SRDetector",
    "ZScoreDetector",
    "OCSVMDetector",
    "MadDetector",
    "SArimaDetector",
]
VALID_MULTIVARIATE_MODLS = [
    "xStreamDetector",
    "RShashDetector",
    "HSTreeDetector",
    "LodaDetector",
    "OCSVMDetector",
    "RrcfDetector",
]

VALID_ENSEMBLE_MODELS = ["WeightEnsemble", "VoteEnsemble"]


@unique
class EnsembleModels(str, Enum):
    WEIGHT = "WeightEnsemble"
    VOTE = "VoteEnsemble"


class Inspector:
    def __init__(self) -> None:
        self.topic = "Collector"
        self.begin_timestamp = None
        self.end_timestamp = None
        self.messages = []
        self.anomalies = []

        logger.debug(f"Initializing Inspector...")
        logger.debug(f"Calling KafkaConsumeHandler(topic='Inspect')...")
        self.kafka_consume_handler = KafkaConsumeHandler(topic="Inspect")
        logger.debug(f"Calling KafkaProduceHandler(transactional_id='Inspect')...")
        self.kafka_produce_handler = KafkaProduceHandler(transactional_id="Inspect")
        logger.debug(f"Initialized Inspector.")

    def get_and_fill_data(self) -> None:
        """Consumes data from KafkaConsumeHandler and stores it for processing."""
        logger.debug("Getting and filling data...")
        if self.messages:
            logger.warning(
                "Inspector is busy: Not consuming new messages. Wait for the Inspector to finish the "
                "current workload."
            )
            return

        logger.debug(
            "Inspector is not busy: Calling KafkaConsumeHandler to consume new JSON messages..."
        )
        key, data = self.kafka_consume_handler.consume_and_return_json_data()

        if data:
            # TODO Fix convertion of data
            self.begin_timestamp = datetime.strptime(
                data.get("begin_timestamp"), TIMESTAMP_FORMAT
            )
            self.end_timestamp = datetime.strptime(
                data.get("end_timestamp"), TIMESTAMP_FORMAT
            )
            self.messages = [ast.literal_eval(item) for item in data.get("data")]
            self.key = key

        if not self.messages:
            logger.info(
                "Received message:\n"
                f"    ⤷  Empty data field: No unfiltered data available. Belongs to subnet_id {key}."
            )
        else:
            logger.info(
                "Received message:\n"
                f"    ⤷  Contains data field of {len(self.messages)} message(s). Belongs to subnet_id {key}."
            )

        logger.debug("Received consumer message as json data.")
        logger.debug(f"(data={self.messages})")

    def clear_data(self):
        """Clears the data in the internal data structures."""
        self.messages = []
        self.anomalies = []
        self.X = []
        self.begin_timestamp = None
        self.end_timestamp = None
        logger.debug("Cleared messages and timestamps. Inspector is now available.")

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
            [
                np.datetime64(datetime.strptime(item["timestamp"], TIMESTAMP_FORMAT))
                for item in messages
            ]
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
            max_date + np.timedelta64(TIME_RANGE, TIME_TYPE),
            np.timedelta64(TIME_RANGE, TIME_TYPE),
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
                    ((timestamps[idx] - min_date) // TIME_RANGE)
                    .astype(f"timedelta64[{TIME_TYPE}]")
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
            [
                np.datetime64(datetime.strptime(item["timestamp"], TIMESTAMP_FORMAT))
                for item in messages
            ]
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
            max_date + np.timedelta64(TIME_RANGE, TIME_TYPE),
            np.timedelta64(TIME_RANGE, TIME_TYPE),
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
                ((unique_times - min_date) // TIME_RANGE)
                .astype(f"timedelta64[{TIME_TYPE}]")
                .astype(int)
            )
            counts[time_indices] = unique_counts
        else:
            logger.warning("Empty messages to inspect.")

        logger.debug("Reshape into the required shape (n, 1)")
        return counts.reshape(-1, 1)

    def inspect(self):
        """Runs anomaly detection on given StreamAD Model on either univariate, multivariate data, or as an ensemble."""
        if len(MODELS) == 0:
            logger.warning("No model ist set!")
            raise NotImplementedError(f"No model is set!")
        match MODE:
            case "univariate":
                if len(MODELS) > 1:
                    logger.warning(
                        f"Model List longer than 1. Only the first one is taken: {MODELS[0]['model']}!"
                    )
                self._inspect_univariate(MODELS[0])
            case "multivariate":
                if len(MODELS) > 1:
                    logger.warning(
                        f"Model List longer than 1. Only the first one is taken: {MODELS[0]['model']}!"
                    )
                self._inspect_multivariate(MODELS[0])
            case "ensemble":
                self._inspect_ensemble(MODELS)
            case _:
                logger.warning(f"Mode {MODE} is not supported!")
                raise NotImplementedError(f"Mode {MODE} is not supported!")

    def _inspect_multivariate(self, model: str):
        logger.debug(f"Load Model: {model['model']} from {model['module']}.")
        if not model["model"] in VALID_MULTIVARIATE_MODLS:
            logger.error(f"Model {model} is not a valid univariate model.")
            raise NotImplementedError(f"Model {model} is not a valid univariate model.")

        module = importlib.import_module(model["module"])
        module_model = getattr(module, model["model"])
        self.model = module_model(**model["model_args"])

        logger.debug("Inspecting data...")

        X_1 = self._mean_packet_size(
            self.messages, self.begin_timestamp, self.end_timestamp
        )
        X_2 = self._count_errors(
            self.messages, self.begin_timestamp, self.end_timestamp
        )

        self.X = np.concatenate((X_1, X_2), axis=1)

        ds = CustomDS(self.X, self.X)
        stream = StreamGenerator(ds.data)

        for x in stream.iter_item():
            score = self.model.fit_score(x)
            if score != None:
                self.anomalies.append(score)
            else:
                self.anomalies.append(0)

    def _inspect_ensemble(self, models: str):
        logger.debug(f"Load Model: {ENSEMBLE['model']} from {ENSEMBLE['module']}.")
        if not ENSEMBLE["model"] in VALID_ENSEMBLE_MODELS:
            logger.error(f"Model {ENSEMBLE} is not a valid univariate model.")
            raise NotImplementedError(
                f"Model {ENSEMBLE} is not a valid univariate model."
            )

        module = importlib.import_module(ENSEMBLE["module"])
        module_model = getattr(module, ENSEMBLE["model"])
        ensemble = module_model(**ENSEMBLE["model_args"])

        self.X = self._count_errors(
            self.messages, self.begin_timestamp, self.end_timestamp
        )

        ds = CustomDS(self.X, self.X)
        stream = StreamGenerator(ds.data)

        self.model = []
        for model in models:
            logger.debug(f"Load Model: {model['model']} from {model['module']}.")
            if not model["model"] in VALID_UNIVARIATE_MODELS:
                logger.error(f"Model {models} is not a valid univariate model.")
                raise NotImplementedError(
                    f"Model {models} is not a valid univariate model."
                )

            module = importlib.import_module(model["module"])
            module_model = getattr(module, model["model"])
            self.model.append(module_model(**model["model_args"]))
        for x in stream.iter_item():
            # Fit all models in ensemble
            for models in self.model:
                models.fit_score(x)
            score = ensemble.ensemble([model for model in self.model])
            if score != None:
                self.anomalies.append(score)
            else:
                self.anomalies.append(0)

    def _inspect_univariate(self, model: str):
        """Runs anomaly detection on given StreamAD Model on univariate data.
        Errors are count in the time window and fit model to retrieve scores.

        Args:
            model (BaseDetector): StreamAD model.
            model_args (dict): Arguments passed to the StreamAD model.
        """

        logger.debug(f"Load Model: {model['model']} from {model['module']}.")
        if not model["model"] in VALID_UNIVARIATE_MODELS:
            logger.error(f"Model {model} is not a valid univariate model.")
            raise NotImplementedError(f"Model {model} is not a valid univariate model.")

        module = importlib.import_module(model["module"])
        module_model = getattr(module, model["model"])
        self.model = module_model(**model["model_args"])

        logger.debug("Inspecting data...")

        self.X = self._count_errors(
            self.messages, self.begin_timestamp, self.end_timestamp
        )

        ds = CustomDS(self.X, self.X)
        stream = StreamGenerator(ds.data)

        for x in stream.iter_item():
            score = self.model.fit_score(x)
            if score != None:
                self.anomalies.append(score)
            else:
                self.anomalies.append(0)

    def send_data(self):
        total_anomalies = np.count_nonzero(
            np.greater_equal(np.array(self.anomalies), SCORE_THRESHOLD)
        )
        if total_anomalies / len(self.X) > ANOMALY_THRESHOLD:
            logger.debug("Sending data to KafkaProduceHandler...")
            logger.info("Sending anomalies to detector for further analysation.")
            data_to_send = {
                "begin_timestamp": self.begin_timestamp.strftime(TIMESTAMP_FORMAT),
                "end_timestamp": self.end_timestamp.strftime(TIMESTAMP_FORMAT),
                "data": self.messages,
            }
            self.kafka_produce_handler.send(
                topic=self.topic,
                data=json.dumps(data_to_send),
                key=self.key,
            )


def main(one_iteration: bool = False):
    """
    Creates the :class:`Inspector` instance. Starts a loop that continuously fetches data. Actual functionality
    follows.

    Args:
        one_iteration (bool): For testing purposes: stops loop after one iteration

    Raises:
        KeyboardInterrupt: Execution interrupted by user. Closes down the :class:`LogCollector` instance.
    """
    logger.info("Starting Inspector...")
    inspector = Inspector()
    logger.info(f"Inspector is running.")

    iterations = 0

    while True:
        if one_iteration and iterations > 0:
            break
        iterations += 1

        try:
            logger.debug("Before getting and filling data")
            inspector.get_and_fill_data()
            logger.debug("After getting and filling data")
            logger.debug("Start anomaly detection")
            inspector.inspect()
            logger.debug("Send data to detector")
            inspector.send_data()
        except KafkaMessageFetchException as e:  # pragma: no cover
            logger.debug(e)
        except IOError as e:
            logger.error(e)
            raise e
        except ValueError as e:
            logger.debug(e)
        except KeyboardInterrupt:
            logger.info("Closing down Inspector...")
            break
        finally:
            inspector.clear_data()


if __name__ == "__main__":  # pragma: no cover
    main()
