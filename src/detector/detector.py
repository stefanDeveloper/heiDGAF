import datetime
import hashlib
import json
import os
import pickle
import sys
import tempfile

import math
import numpy as np
import requests
from numpy import median

sys.path.append(os.getcwd())
from src.base.clickhouse_kafka_sender import ClickHouseKafkaSender
from src.base.utils import setup_config
from src.base.kafka_handler import (
    ExactlyOnceKafkaConsumeHandler,
    KafkaMessageFetchException,
)
from src.base.log_config import get_logger

module_name = "data_analysis.detector"
logger = get_logger(module_name)

BUF_SIZE = 65536  # let's read stuff in 64kb chunks!

config = setup_config()
MODEL = config["pipeline"]["data_analysis"]["detector"]["model"]
CHECKSUM = config["pipeline"]["data_analysis"]["detector"]["checksum"]
MODEL_BASE_URL = config["pipeline"]["data_analysis"]["detector"]["base_url"]
THRESHOLD = config["pipeline"]["data_analysis"]["detector"]["threshold"]
CONSUME_TOPIC = config["environment"]["kafka_topics"]["pipeline"][
    "inspector_to_detector"
]


class WrongChecksum(Exception):  # pragma: no cover
    """Raises when model checksum validation fails."""

    pass


class Detector:
    """Main component of the Data Analysis stage to perform anomaly detection

    Processes suspicious batches from the Inspector using configurable ML models to classify
    DNS requests as benign or malicious. Downloads and validates models from a remote server,
    extracts features from domain names, calculates probability scores, and generates alerts
    when malicious requests are detected above the configured threshold.
    """

    def __init__(self) -> None:
        self.suspicious_batch_id = None
        self.key = None
        self.messages = []
        self.warnings = []
        self.begin_timestamp = None
        self.end_timestamp = None
        self.model_path = os.path.join(
            tempfile.gettempdir(), f"{MODEL}_{CHECKSUM}_model.pickle"
        )
        self.scaler_path = os.path.join(
            tempfile.gettempdir(), f"{MODEL}_{CHECKSUM}_scaler.pickle"
        )

        self.kafka_consume_handler = ExactlyOnceKafkaConsumeHandler(CONSUME_TOPIC)

        self.model, self.scaler = self._get_model()

        # databases
        self.suspicious_batch_timestamps = ClickHouseKafkaSender(
            "suspicious_batch_timestamps"
        )
        self.alerts = ClickHouseKafkaSender("alerts")
        self.logline_timestamps = ClickHouseKafkaSender("logline_timestamps")
        self.fill_levels = ClickHouseKafkaSender("fill_levels")

        self.fill_levels.insert(
            dict(
                timestamp=datetime.datetime.now(),
                stage=module_name,
                entry_type="total_loglines",
                entry_count=0,
            )
        )

    def get_and_fill_data(self) -> None:
        """Consumes suspicious batches from Kafka and stores them for analysis.

        Fetches suspicious batch data from the Inspector via Kafka and stores it in internal
        data structures. If the Detector is already busy processing data, consumption is
        skipped with a warning. Updates database entries for monitoring and logging purposes.
        """
        if self.messages:
            logger.warning(
                "Detector is busy: Not consuming new messages. Wait for the Detector to finish the "
                "current workload."
            )
            return

        key, data = self.kafka_consume_handler.consume_as_object()

        if data.data:
            self.suspicious_batch_id = data.batch_id
            self.begin_timestamp = data.begin_timestamp
            self.end_timestamp = data.end_timestamp
            self.messages = data.data
            self.key = key

        self.suspicious_batch_timestamps.insert(
            dict(
                suspicious_batch_id=self.suspicious_batch_id,
                client_ip=key,
                stage=module_name,
                status="in_process",
                timestamp=datetime.datetime.now(),
                is_active=True,
                message_count=len(self.messages),
            )
        )

        self.fill_levels.insert(
            dict(
                timestamp=datetime.datetime.now(),
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

    def _sha256sum(self, file_path: str) -> str:
        """Calculates SHA256 checksum for model file validation.

        Args:
            file_path (str): Path to the model file to validate.

        Returns:
            str: SHA256 hexadecimal digest of the file.
        """
        h = hashlib.sha256()

        with open(file_path, "rb") as file:
            while True:
                # Reading is buffered, so we can read smaller chunks.
                chunk = file.read(h.block_size)
                if not chunk:
                    break
                h.update(chunk)

        return h.hexdigest()

    def _get_model(self):
        """Downloads and loads ML model and scaler from remote server.

        Retrieves the configured model and scaler files from the remote server if not
        already present locally. Validates model integrity using SHA256 checksum and
        loads the pickled model and scaler objects for inference.

        Returns:
            tuple: Trained ML model and data scaler objects.

        Raises:
            WrongChecksum: If model checksum validation fails.
        """
        logger.info(f"Get model: {MODEL} with checksum {CHECKSUM}")
        if not os.path.isfile(self.model_path):
            response = requests.get(
                f"{MODEL_BASE_URL}/files/?p=%2F{MODEL}/{CHECKSUM}/{MODEL}.pickle&dl=1"
            )
            logger.info(
                f"{MODEL_BASE_URL}/files/?p=%2F{MODEL}/{CHECKSUM}/{MODEL}.pickle&dl=1"
            )
            response.raise_for_status()

            with open(self.model_path, "wb") as f:
                f.write(response.content)

        if not os.path.isfile(self.scaler_path):
            response = requests.get(
                f"{MODEL_BASE_URL}/files/?p=%2F{MODEL}/{CHECKSUM}/scaler.pickle&dl=1"
            )
            logger.info(
                f"{MODEL_BASE_URL}/files/?p=%2F{MODEL}/{CHECKSUM}/scaler.pickle&dl=1"
            )
            response.raise_for_status()

            with open(self.scaler_path, "wb") as f:
                f.write(response.content)

        # Check file sha256
        local_checksum = self._sha256sum(self.model_path)

        if local_checksum != CHECKSUM:
            logger.warning(
                f"Checksum {CHECKSUM} SHA256 is not equal with new checksum {local_checksum}!"
            )
            raise WrongChecksum(
                f"Checksum {CHECKSUM} SHA256 is not equal with new checksum {local_checksum}!"
            )

        with open(self.model_path, "rb") as input_file:
            clf = pickle.load(input_file)

        with open(self.scaler_path, "rb") as input_file:
            scaler = pickle.load(input_file)

        return clf, scaler

    def clear_data(self) -> None:
        """Clears all data from internal data structures.

        Resets messages, timestamps, and warnings to prepare the Detector
        for processing the next suspicious batch.
        """
        self.messages = []
        self.begin_timestamp = None
        self.end_timestamp = None
        self.warnings = []

    def _get_features(self, query: str) -> np.ndarray:
        """Extracts feature vector from domain name for ML model inference.

        Computes various statistical and linguistic features from the domain name
        including label lengths, character frequencies, entropy measures, and
        counts of different character types across domain name levels.

        Args:
            query (str): Domain name string to extract features from.

        Returns:
            numpy.ndarray: Feature vector ready for ML model prediction.
        """

        # Splitting by dots to calculate label length and max length
        query = query.strip(".")
        label_parts = query.split(".")

        levels = {
            "fqdn": query,
            "secondleveldomain": label_parts[-2] if len(label_parts) >= 2 else "",
            "thirdleveldomain": (
                ".".join(label_parts[:-2]) if len(label_parts) > 2 else ""
            ),
        }

        label_length = len(label_parts)
        parts = query.split(".")
        label_max = len(max(parts, key=str)) if parts else 0
        label_average = len(query)

        basic_features = np.array(
            [label_length, label_max, label_average], dtype=np.float64
        )

        alc = "abcdefghijklmnopqrstuvwxyz"
        query_len = len(query)
        freq = np.array(
            [query.lower().count(c) / query_len if query_len > 0 else 0.0 for c in alc],
            dtype=np.float64,
        )

        logger.debug("Get full, alpha, special, and numeric count.")

        def calculate_counts(level: str) -> np.ndarray:
            if not level:
                return np.array([0.0, 0.0, 0.0, 0.0], dtype=np.float64)

            full_count = len(level) / len(level)
            alpha_ratio = sum(c.isalpha() for c in level) / len(level)
            numeric_ratio = sum(c.isdigit() for c in level) / len(level)
            special_ratio = sum(
                not c.isalnum() and not c.isspace() for c in level
            ) / len(level)

            return np.array(
                [full_count, alpha_ratio, numeric_ratio, special_ratio],
                dtype=np.float64,
            )

        fqdn_counts = calculate_counts(levels["fqdn"])
        third_counts = calculate_counts(levels["thirdleveldomain"])
        second_counts = calculate_counts(levels["secondleveldomain"])

        level_features = np.hstack([third_counts, second_counts, fqdn_counts])

        def calculate_entropy(s: str) -> float:
            if len(s) == 0:
                return 0.0
            probs = [s.count(c) / len(s) for c in dict.fromkeys(s)]
            return -sum(p * math.log(p, 2) for p in probs)

        logger.debug("Start entropy calculation")
        entropy_features = np.array(
            [
                calculate_entropy(levels["fqdn"]),
                calculate_entropy(levels["thirdleveldomain"]),
                calculate_entropy(levels["secondleveldomain"]),
            ],
            dtype=np.float64,
        )

        logger.debug("Entropy features calculated")

        all_features = np.concatenate(
            [basic_features, freq, level_features, entropy_features]
        )

        logger.debug("Finished data transformation")

        return all_features.reshape(1, -1)

    def detect(self) -> None:  # pragma: no cover
        """Analyzes DNS requests and identifies malicious domains.

        Processes each DNS request in the current batch by extracting features,
        running ML model prediction, and collecting warnings for requests that
        exceed the configured maliciousness threshold.
        """
        logger.info("Start detecting malicious requests.")
        for message in self.messages:
            # TODO predict all messages
            # TODO use scalar: self.scaler.transform(self._get_features(message["domain_name"]))
            y_pred = self.model.predict_proba(
                self._get_features(message["domain_name"])
            )
            logger.info(f"Prediction: {y_pred}")
            if np.argmax(y_pred, axis=1) == 1 and y_pred[0][1] > THRESHOLD:
                logger.info("Append malicious request to warning.")
                warning = {
                    "request": message,
                    "probability": float(y_pred[0][1]),
                    "model": MODEL,
                    "sha256": CHECKSUM,
                }
                self.warnings.append(warning)

    def send_warning(self) -> None:
        """Generates and stores alerts for detected malicious requests.

        Creates comprehensive alert records from accumulated warnings including
        overall risk scores, individual predictions, and metadata. Stores alerts
        in the database and updates batch processing status. If no warnings are
        present, marks the batch as filtered out.
        """
        logger.info("Store alert.")
        if len(self.warnings) > 0:
            overall_score = median(
                [warning["probability"] for warning in self.warnings]
            )
            alert = {"overall_score": overall_score, "result": self.warnings}

            logger.info(f"Add alert: {alert}")
            with open(os.path.join(tempfile.gettempdir(), "warnings.json"), "a+") as f:
                json.dump(alert, f)
                f.write("\n")

            self.alerts.insert(
                dict(
                    client_ip=self.key,
                    alert_timestamp=datetime.datetime.now(),
                    suspicious_batch_id=self.suspicious_batch_id,
                    overall_score=overall_score,
                    domain_names=json.dumps(
                        [warning["request"] for warning in self.warnings]
                    ),
                    result=json.dumps(self.warnings),
                )
            )

            self.suspicious_batch_timestamps.insert(
                dict(
                    suspicious_batch_id=self.suspicious_batch_id,
                    client_ip=self.key,
                    stage=module_name,
                    status="finished",
                    timestamp=datetime.datetime.now(),
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
                        status="detected",
                        timestamp=datetime.datetime.now(),
                        is_active=False,
                    )
                )
        else:
            logger.info("No warning produced.")

            self.suspicious_batch_timestamps.insert(
                dict(
                    suspicious_batch_id=self.suspicious_batch_id,
                    client_ip=self.key,
                    stage=module_name,
                    status="filtered_out",
                    timestamp=datetime.datetime.now(),
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
                        timestamp=datetime.datetime.now(),
                        is_active=False,
                    )
                )

        self.fill_levels.insert(
            dict(
                timestamp=datetime.datetime.now(),
                stage=module_name,
                entry_type="total_loglines",
                entry_count=0,
            )
        )


def main(one_iteration: bool = False) -> None:  # pragma: no cover
    """Creates and runs the Detector instance in a continuous processing loop.

    Initializes the Detector and starts the main processing loop that continuously
    fetches suspicious batches from Kafka, performs malicious domain detection,
    and generates alerts. Handles various exceptions gracefully and ensures
    proper cleanup of data structures.

    Args:
        one_iteration (bool): For testing purposes - stops loop after one iteration.

    Raises:
        KeyboardInterrupt: Execution interrupted by user.
    """
    logger.info("Starting Detector...")
    detector = Detector()
    logger.info(f"Detector is running.")

    iterations = 0

    while True:
        if one_iteration and iterations > 0:
            break
        iterations += 1

        try:
            logger.debug("Before getting and filling data")
            detector.get_and_fill_data()
            logger.debug("Inspect Data")
            detector.detect()
            logger.debug("Send warnings")
            detector.send_warning()
        except KafkaMessageFetchException as e:  # pragma: no cover
            logger.debug(e)
        except IOError as e:
            logger.error(e)
            raise e
        except ValueError as e:
            logger.debug(e)
        except KeyboardInterrupt:
            logger.info("Closing down Detector...")
            break
        finally:
            detector.clear_data()


if __name__ == "__main__":  # pragma: no cover
    main()
