import importlib
import os
import sys
import uuid
from datetime import datetime
from enum import Enum, unique

import marshmallow_dataclass
import numpy as np
from streamad.util import StreamGenerator, CustomDS

sys.path.append(os.getcwd())
from src.base.clickhouse_kafka_sender import ClickHouseKafkaSender
from src.base.data_classes.batch import Batch
from src.base.utils import setup_config
from src.base.kafka_handler import (
    ExactlyOnceKafkaConsumeHandler,
    ExactlyOnceKafkaProduceHandler,
    KafkaMessageFetchException,
)
from src.base.log_config import get_logger

module_name = "data_inspection.inspector"
logger = get_logger(module_name)

config = setup_config()
MODE = config["pipeline"]["data_inspection"]["inspector"]["mode"]
ENSEMBLE = config["pipeline"]["data_inspection"]["inspector"]["ensemble"]
MODELS = config["pipeline"]["data_inspection"]["inspector"]["models"]
ANOMALY_THRESHOLD = config["pipeline"]["data_inspection"]["inspector"][
    "anomaly_threshold"
]
SCORE_THRESHOLD = config["pipeline"]["data_inspection"]["inspector"]["score_threshold"]
TIME_TYPE = config["pipeline"]["data_inspection"]["inspector"]["time_type"]
TIME_RANGE = config["pipeline"]["data_inspection"]["inspector"]["time_range"]
CONSUME_TOPIC = config["environment"]["kafka_topics"]["pipeline"][
    "prefilter_to_inspector"
]
PRODUCE_TOPIC = config["environment"]["kafka_topics"]["pipeline"][
    "inspector_to_detector"
]
KAFKA_BROKERS = ",".join(
    [
        f"{broker['hostname']}:{broker['port']}"
        for broker in config["environment"]["kafka_brokers"]
    ]
)

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


@unique
class EnsembleModels(str, Enum):
    """Available ensemble models for combining multiple anomaly detectors"""

    WEIGHT = "WeightEnsemble"
    VOTE = "VoteEnsemble"


class Inspector:
    """Main component of the Data Inspection stage to detect anomalies in request batches

    Analyzes batches of DNS requests using configurable streaming anomaly detection models.
    Supports univariate, multivariate, and ensemble detection modes. Processes time series
    features from DNS request patterns to identify suspicious network behavior and forwards
    anomalous batches to the Detector for further analysis.
    """

    def __init__(self) -> None:
        self.batch_id = None
        self.X = None
        self.key = None
        self.begin_timestamp = None
        self.end_timestamp = None

        self.messages = []
        self.anomalies = []

        self.kafka_consume_handler = ExactlyOnceKafkaConsumeHandler(CONSUME_TOPIC)
        self.kafka_produce_handler = ExactlyOnceKafkaProduceHandler()

        # databases
        self.batch_timestamps = ClickHouseKafkaSender("batch_timestamps")
        self.suspicious_batch_timestamps = ClickHouseKafkaSender(
            "suspicious_batch_timestamps"
        )
        self.suspicious_batches_to_batch = ClickHouseKafkaSender(
            "suspicious_batches_to_batch"
        )
        self.logline_timestamps = ClickHouseKafkaSender("logline_timestamps")
        self.fill_levels = ClickHouseKafkaSender("fill_levels")

        self.fill_levels.insert(
            dict(
                timestamp=datetime.now(),
                stage=module_name,
                entry_type="total_loglines",
                entry_count=0,
            )
        )

    def get_and_fill_data(self) -> None:
        """Consumes data from Kafka and stores it for processing.

        Fetches batch data from the configured Kafka topic and stores it in internal data structures.
        If the Inspector is already busy processing data, the consumption is skipped with a warning.
        Logs batch information and updates database entries for monitoring purposes.
        """
        if self.messages:
            logger.warning(
                "Inspector is busy: Not consuming new messages. Wait for the Inspector to finish the "
                "current workload."
            )
            return

        key, data = self.kafka_consume_handler.consume_as_object()

        if data:
            self.batch_id = data.batch_id
            self.begin_timestamp = data.begin_timestamp
            self.end_timestamp = data.end_timestamp
            self.messages = data.data
            self.key = key

        self.batch_timestamps.insert(
            dict(
                batch_id=self.batch_id,
                stage=module_name,
                status="in_process",
                timestamp=datetime.now(),
                is_active=True,
                message_count=len(self.messages),
            )
        )

        self.fill_levels.insert(
            dict(
                timestamp=datetime.now(),
                stage=module_name,
                entry_type="total_loglines",
                entry_count=len(self.messages),
            )
        )

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

    def clear_data(self) -> None:
        """Clears all data from internal data structures.

        Resets messages, anomalies, feature matrix, and timestamps to prepare
        the Inspector for processing the next batch of data.
        """
        self.messages = []
        self.anomalies = []
        self.X = []
        self.begin_timestamp = None
        self.end_timestamp = None
        logger.debug("Cleared messages and timestamps. Inspector is now available.")

    def _mean_packet_size(
        self, messages: list, begin_timestamp, end_timestamp
    ) -> np.ndarray:
        """Calculates mean packet size per time step for time series analysis.

        Computes the average packet size for each time step in a given time window.
        Time steps are configurable via "time_type" and "time_range" in config.yaml.
        Default time step is 1 ms.

        Args:
            messages (list): Messages from KafkaConsumeHandler containing size information.
            begin_timestamp (datetime): Start timestamp of the batch time window.
            end_timestamp (datetime): End timestamp of the batch time window.

        Returns:
            numpy.ndarray: 2-D numpy array with mean packet sizes for each time step.
        """
        logger.debug("Convert timestamps to numpy datetime64")
        timestamps = np.array(
            [
                np.datetime64(datetime.fromisoformat(item["timestamp"]))
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

    def _count_errors(
        self, messages: list, begin_timestamp, end_timestamp
    ) -> np.ndarray:
        """Counts message occurrences per time step for time series analysis.

        Counts the number of messages occurring in each time step within a given time window.
        Time steps are configurable via "time_type" and "time_range" in config.yaml.
        Default time step is 1 ms.

        Args:
            messages (list): Messages from KafkaConsumeHandler containing timestamp information.
            begin_timestamp (datetime): Start timestamp of the batch time window.
            end_timestamp (datetime): End timestamp of the batch time window.

        Returns:
            numpy.ndarray: 2-D numpy array with message counts for each time step.
        """
        logger.debug("Convert timestamps to numpy datetime64")
        timestamps = np.array(
            [
                np.datetime64(datetime.fromisoformat(item["timestamp"]))
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

    def inspect(self) -> None:
        """Runs anomaly detection using configured StreamAD models.

        Executes anomaly detection based on the configured mode (univariate, multivariate, or ensemble).
        Validates model configuration and delegates to the appropriate inspection method.

        Raises:
            NotImplementedError: If no models are configured or mode is unsupported.
        """
        if MODELS == None or len(MODELS) == 0:
            logger.warning("No model ist set!")
            raise NotImplementedError(f"No model is set!")
        if len(MODELS) > 1:
            logger.warning(
                f"Model List longer than 1. Only the first one is taken: {MODELS[0]['model']}!"
            )
        self._get_models(MODELS)
        match MODE:
            case "univariate":
                self._inspect_univariate()
            case "multivariate":
                self._inspect_multivariate()
            case "ensemble":
                self._get_ensemble()
                self._inspect_ensemble()
            case _:
                logger.warning(f"Mode {MODE} is not supported!")
                raise NotImplementedError(f"Mode {MODE} is not supported!")

    def _inspect_multivariate(self) -> None:
        """Performs multivariate anomaly detection using StreamAD model.

        Combines mean packet size and message count time series to create a multivariate
        feature matrix for anomaly detection. Computes anomaly scores for each time step
        using the configured multivariate StreamAD model.
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

    def _inspect_ensemble(self) -> None:
        """Performs ensemble anomaly detection using multiple StreamAD models.

        Uses message count time series and combines scores from multiple StreamAD models
        through ensemble methods (Weight or Vote). Computes final ensemble scores
        for each time step in the data.
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

    def _inspect_univariate(self) -> None:
        """Performs univariate anomaly detection using StreamAD model.

        Uses message count time series as a single feature for anomaly detection.
        Computes anomaly scores for each time step using the configured
        univariate StreamAD model.
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

    def _get_models(self, models: list) -> None:
        """Loads and initializes StreamAD detection models.

        Dynamically imports and instantiates the configured StreamAD models based on the
        detection mode (univariate, multivariate, or ensemble). Validates model compatibility
        with the selected mode and initializes models with their configuration parameters.

        Args:
            models (list): List of model configurations containing module and model information.

        Raises:
            NotImplementedError: If a model is not compatible with the selected mode.
        """
        if hasattr(self, "models") and self.models != None and self.models != []:
            logger.info("All models have been successfully loaded!")
            return

        self.models = []
        for model in models:
            if MODE == "univariate" or MODE == "ensemble":
                logger.debug(f"Load Model: {model['model']} from {model['module']}.")
                if not model["model"] in VALID_UNIVARIATE_MODELS:
                    logger.error(
                        f"Model {models} is not a valid univariate or ensemble model."
                    )
                    raise NotImplementedError(
                        f"Model {models} is not a valid univariate or ensemble model."
                    )
            if MODE == "multivariate":
                logger.debug(f"Load Model: {model['model']} from {model['module']}.")
                if not model["model"] in VALID_MULTIVARIATE_MODELS:
                    logger.error(f"Model {model} is not a valid multivariate model.")
                    raise NotImplementedError(
                        f"Model {model} is not a valid multivariate model."
                    )

            module = importlib.import_module(model["module"])
            module_model = getattr(module, model["model"])
            self.models.append(module_model(**model["model_args"]))

    def _get_ensemble(self) -> None:
        """Loads and initializes ensemble model for combining multiple detectors.

        Dynamically imports and instantiates the configured ensemble model (Weight or Vote)
        that combines scores from multiple StreamAD models. Validates that the ensemble
        model is supported and initializes it with configuration parameters.

        Raises:
            NotImplementedError: If the ensemble model is not supported.
        """
        logger.debug(f"Load Model: {ENSEMBLE['model']} from {ENSEMBLE['module']}.")
        if not ENSEMBLE["model"] in VALID_ENSEMBLE_MODELS:
            logger.error(f"Model {ENSEMBLE} is not a valid ensemble model.")
            raise NotImplementedError(
                f"Model {ENSEMBLE} is not a valid ensemble model."
            )

        if hasattr(self, "ensemble") and self.ensemble != None:
            logger.info("Ensemble have been successfully loaded!")
            return

        module = importlib.import_module(ENSEMBLE["module"])
        module_model = getattr(module, ENSEMBLE["model"])
        self.ensemble = module_model(**ENSEMBLE["model_args"])

    def send_data(self) -> None:
        """Forwards anomalous data to the Detector for further analysis.

        Evaluates anomaly scores against the configured thresholds. If the proportion of
        anomalous time steps exceeds the threshold, groups messages by client IP and
        forwards each group as a suspicious batch to the Detector via Kafka. Otherwise,
        logs the batch as filtered out and updates monitoring databases.
        """
        total_anomalies = np.count_nonzero(
            np.greater_equal(np.array(self.anomalies), SCORE_THRESHOLD)
        )
        if total_anomalies / len(self.X) > ANOMALY_THRESHOLD:  # subnet is suspicious
            logger.info("Sending anomalies to detector for further analysis.")
            buckets = {}

            for message in self.messages:
                if message["client_ip"] in buckets.keys():
                    buckets[message["client_ip"]].append(message)
                else:
                    buckets[message["client_ip"]] = []
                    buckets.get(message["client_ip"]).append(message)

            for key, value in buckets.items():
                logger.info(f"Sending anomalies to detector for {key}.")
                logger.info(f"Sending anomalies to detector for {value}.")

                suspicious_batch_id = uuid.uuid4()  # generate new suspicious_batch_id

                self.suspicious_batches_to_batch.insert(
                    dict(
                        suspicious_batch_id=suspicious_batch_id,
                        batch_id=self.batch_id,
                    )
                )

                data_to_send = {
                    "batch_id": suspicious_batch_id,
                    "begin_timestamp": self.begin_timestamp,
                    "end_timestamp": self.end_timestamp,
                    "data": value,
                }

                batch_schema = marshmallow_dataclass.class_schema(Batch)()

                self.suspicious_batch_timestamps.insert(
                    dict(
                        suspicious_batch_id=suspicious_batch_id,
                        client_ip=key,
                        stage=module_name,
                        status="finished",
                        timestamp=datetime.now(),
                        is_active=True,
                        message_count=len(value),
                    )
                )

                self.kafka_produce_handler.produce(
                    topic=PRODUCE_TOPIC,
                    data=batch_schema.dumps(data_to_send),
                    key=key,
                )
        else:  # subnet is not suspicious
            self.batch_timestamps.insert(
                dict(
                    batch_id=self.batch_id,
                    stage=module_name,
                    status="filtered_out",
                    timestamp=datetime.now(),
                    is_active=False,
                    message_count=len(self.messages),
                )
            )

            logline_ids = set()
            for message in self.messages:
                logline_ids.add(message["logline_id"])

            for logline_id in logline_ids:
                self.logline_timestamps.insert(
                    dict(
                        logline_id=logline_id,
                        stage=module_name,
                        status="filtered_out",
                        timestamp=datetime.now(),
                        is_active=False,
                    )
                )

        self.fill_levels.insert(
            dict(
                timestamp=datetime.now(),
                stage=module_name,
                entry_type="total_loglines",
                entry_count=0,
            )
        )


def main(one_iteration: bool = False) -> None:
    """Creates and runs the Inspector instance in a continuous processing loop.

    Initializes the Inspector and starts the main processing loop that continuously
    fetches batches from Kafka, performs anomaly detection, and forwards suspicious
    batches to the Detector. Handles various exceptions gracefully and ensures
    proper cleanup of data structures.

    Args:
        one_iteration (bool): For testing purposes - stops loop after one iteration.

    Raises:
        KeyboardInterrupt: Execution interrupted by user.
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
