import importlib
import os
import sys
import uuid
from datetime import datetime
from enum import Enum, unique
import asyncio

import marshmallow_dataclass
import numpy as np
from streamad.util import StreamGenerator, CustomDS

sys.path.append(os.getcwd())
from src.base.clickhouse_kafka_sender import ClickHouseKafkaSender
from src.base.data_classes.batch import Batch
from src.base.utils import setup_config, get_zeek_sensor_topic_base_names
from src.base.kafka_handler import (
    SimpleKafkaConsumeHandler,
    SimpleKafkaProduceHandler,
    KafkaMessageFetchException,
)
from src.base.log_config import get_logger

module_name = "data_inspection.inspector"
logger = get_logger(module_name)

config = setup_config()
PRODUCE_TOPIC_PREFIX = config["environment"]["kafka_topics_prefix"]["pipeline"]["inspector_to_detector"]
CONSUME_TOPIC_PREFIX = config["environment"]["kafka_topics_prefix"]["pipeline"]["prefilter_to_inspector"]
SENSOR_PROTOCOLS = get_zeek_sensor_topic_base_names(config)
PREFILTERS = config["pipeline"]["log_filtering"]
INSPECTORS = config["pipeline"]["data_inspection"]
COLLECTORS = config["pipeline"]["log_collection"]["collectors"]
DETECTORS = config["pipeline"]["data_analysis"]


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
    WEIGHT = "WeightEnsemble"
    VOTE = "VoteEnsemble"


class Inspector:
    """Finds anomalies in a batch of requests and produces it to the ``Detector``."""

    def __init__(self, consume_topic, produce_topics, config) -> None:
        self.mode = config["mode"]
        self.ensemble = config["ensemble"]
        self.model_configurations = config["models"]
        self.anomaly_threshold = config["anomaly_threshold"]
        self.score_threshold = config["score_threshold"]
        self.time_type = config["time_type"]
        self.time_range = config["time_range"]

        self.consume_topic = consume_topic
        self.produce_topics = produce_topics
        self.batch_id = None
        self.X = None
        self.key = None
        self.begin_timestamp = None
        self.end_timestamp = None

        self.messages = []
        self.anomalies = []

        self.kafka_consume_handler = SimpleKafkaConsumeHandler(self.consume_topic)
        self.kafka_produce_handler = SimpleKafkaProduceHandler()

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
        """Consumes data from KafkaConsumeHandler and stores it for processing."""
        if self.messages:
            logger.warning(
                "Inspector is busy: Not consuming new messages. Wait for the Inspector to finish the "
                "current workload."
            )
            return

        key, data = self.kafka_consume_handler.consume_as_object()
        logger.info("got-data")
        if data:
            self.batch_id = data.batch_id
            self.begin_timestamp = data.begin_timestamp
            self.end_timestamp = data.end_timestamp
            self.messages = data.data
            self.key = key
        logger.info("transformed")
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
        logger.info("inserted")
        self.fill_levels.insert(
            dict(
                timestamp=datetime.now(),
                stage=module_name,
                entry_type="total_loglines",
                entry_count=len(self.messages),
            )
        )
        logger.info("inserte2")

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
                np.datetime64(datetime.fromisoformat(item["ts"]))
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
            [
                np.datetime64(datetime.fromisoformat(item["ts"]))
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

    def inspect(self):
        """Runs anomaly detection on given StreamAD Model on either univariate, multivariate data, or as an ensemble."""
        if self.model_configurations == None or len(self.model_configurations) == 0:
            logger.warning("No model ist set!")
            raise NotImplementedError(f"No model is set!")
        if len(self.model_configurations) > 1:
            logger.warning(
                f"Model List longer than 1. Only the first one is taken: {self.model_configurations[0]['model']}!"
            )
        self._get_models(self.model_configurations)
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

    def _get_models(self, models):
        if hasattr(self, "models") and self.models != None and self.models != []:
            logger.info("All models have been successfully loaded!")
            return

        self.models = []
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
            self.models.append(module_model(**model["model_args"]))

    def _get_ensemble(self):
        logger.debug(f"Load Model: {self.ensemble['model']} from {self.ensemble['module']}.")
        if not self.ensemble["model"] in VALID_ENSEMBLE_MODELS:
            logger.error(f"Model {self.ensemble} is not a valid ensemble model.")
            raise NotImplementedError(
                f"Model {self.ensemble} is not a valid ensemble model."
            )

        if hasattr(self, "ensemble") and self.ensemble != None:
            logger.info("Ensemble have been successfully loaded!")
            return

        module = importlib.import_module(self.ensemble["module"])
        module_model = getattr(module, self.ensemble["model"])
        self.ensemble = module_model(**self.ensemble["model_args"])

    def send_data(self):
        """Pass the anomalous data for the detector unit for further processing"""
        total_anomalies = np.count_nonzero(
            np.greater_equal(np.array(self.anomalies), self.score_threshold)
        )
        logger.info(f" {total_anomalies} anomalies found")
        if total_anomalies / len(self.X) > self.anomaly_threshold:  # subnet is suspicious
            logger.info("Sending anomalies to detector for further analysis.")
            buckets = {}
            for message in self.messages:
                if message["src_ip"] in buckets.keys():
                    buckets[message["src_ip"]].append(message)
                else:
                    buckets[message["src_ip"]] = []
                    buckets.get(message["src_ip"]).append(message)

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
                        src_ip=key,
                        stage=module_name,
                        status="finished",
                        timestamp=datetime.now(),
                        is_active=True,
                        message_count=len(value),
                    )
                )
                for topic in self.produce_topics:
                    logger.info(f"now producing for {topic}")
                    self.kafka_produce_handler.produce(
                        topic=topic,
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

    def start(self, one_iteration: bool = False):
        iterations = 0

        while True:
            if one_iteration and iterations > 0:
                break
            iterations += 1

            try:
                logger.debug("Before getting and filling data")
                self.get_and_fill_data()
                logger.debug("After getting and filling data")
                logger.debug("Start anomaly detection")
                self.inspect()
                logger.debug("Send data to detector")
                self.send_data()
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
                self.clear_data()
            
async def main():
    """
    Creates the :class:`Inspector` instance. Starts a loop that continuously fetches data. Actual functionality
    follows.

    Args:
        one_iteration (bool): For testing purposes: stops loop after one iteration

    Raises:
        KeyboardInterrupt: Execution interrupted by user. Closes down the :class:`LogCollector` instance.
    """
    tasks = []
    for inspector in INSPECTORS:
        consume_topic = f"{CONSUME_TOPIC_PREFIX}-{inspector['name']}"
        produce_topics = [f"{PRODUCE_TOPIC_PREFIX}-{detector['name']}" for detector in DETECTORS if detector["inspector_name"] == inspector["name"]]
        logger.info("Starting Inspector...")
        inspector = Inspector(consume_topic=consume_topic, produce_topics=produce_topics, config=inspector)
        
        tasks.append(
            asyncio.create_task(inspector.start())
        )

    await asyncio.gather(*tasks)



if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
