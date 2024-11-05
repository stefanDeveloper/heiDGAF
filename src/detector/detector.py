import hashlib
import json
import os
import pickle
import sys
import tempfile

import numpy as np
import math
import requests
from numpy import median

sys.path.append(os.getcwd())
from src.base.utils import setup_config
from src.base.kafka_handler import (
    KafkaConsumeHandler,
    KafkaMessageFetchException,
)
from src.base.log_config import get_logger

logger = get_logger("data_analysis.detector")

BUF_SIZE = 65536  # let's read stuff in 64kb chunks!

config = setup_config()
MODEL = config["pipeline"]["data_analysis"]["detector"]["model"]
CHECKSUM = config["pipeline"]["data_analysis"]["detector"]["checksum"]
MODEL_BASE_URL = config["pipeline"]["data_analysis"]["detector"]["base_url"]
THRESHOLD = config["pipeline"]["data_analysis"]["detector"]["threshold"]


class WrongChecksum(Exception):  # pragma: no cover
    """
    Exception if Checksum is not equal.
    """

    pass


class Detector:
    """Logs detection with probability score of requests. It runs the provided machine learning model.
    In addition, it returns all individually probabilities of the anomalous batch.
    """

    def __init__(self) -> None:
        self.messages = []
        self.warnings = []
        self.begin_timestamp = None
        self.end_timestamp = None
        self.model_path = os.path.join(
            tempfile.gettempdir(), f"{MODEL}_{CHECKSUM}.pickle"
        )

        logger.debug(f"Initializing Detector...")
        logger.debug(f"Calling KafkaConsumeHandler(topic='Detector')...")
        self.kafka_consume_handler = KafkaConsumeHandler(topic="Detector")

        self.model = self._get_model()

    def get_and_fill_data(self) -> None:
        """Consumes data from KafkaConsumeHandler and stores it for processing."""
        logger.debug("Getting and filling data...")
        if self.messages:
            logger.warning(
                "Detector is busy: Not consuming new messages. Wait for the Detector to finish the "
                "current workload."
            )
            return

        logger.debug(
            "Detector is not busy: Calling KafkaConsumeHandler to consume new JSON messages..."
        )
        key, data = self.kafka_consume_handler.consume_and_return_object()

        if data:
            self.begin_timestamp = data.begin_timestamp
            self.end_timestamp = data.end_timestamp
            self.messages = data.data
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

    def _sha256sum(self, file_path: str) -> str:
        """Return a SHA265 sum check to validate the model.

        Args:
            file_path (str): File path of model.

        Returns:
            str: SHA256 sum
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

    def _get_model(self) -> None:
        """
        Downloads model from server. If model already exists, it returns the current model. In addition, it checks the
        sha256 sum in case a model has been updated.
        """
        logger.info(f"Get model: {MODEL} with checksum {CHECKSUM}")
        if not os.path.isfile(self.model_path):
            response = requests.get(
                f"{MODEL_BASE_URL}/files/?p=%2F{MODEL}_{CHECKSUM}.pickle&dl=1"
            )
            logger.info(f"{MODEL_BASE_URL}/files/?p=%2F{MODEL}_{CHECKSUM}.pickle&dl=1")
            response.raise_for_status()

            with open(self.model_path, "wb") as f:
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

        return clf

    def clear_data(self):
        """Clears the data in the internal data structures."""
        self.messages = []
        self.begin_timestamp = None
        self.end_timestamp = None
        self.warnings = []

    def _get_features(self, query: str):
        """Transform a dataset with new features using numpy.

        Args:
            query (str): A string to process.

        Returns:
            dict: Preprocessed data with computed features.
        """

        # Splitting by dots to calculate label length and max length
        label_parts = query.split(".")
        label_length = len(label_parts)
        label_max = max(len(part) for part in label_parts)
        label_average = len(query.strip("."))

        logger.debug("Get letter frequency")
        alc = "abcdefghijklmnopqrstuvwxyz"
        freq = np.array(
            [query.lower().count(i) / len(query) if len(query) > 0 else 0 for i in alc]
        )

        logger.debug("Get full, alpha, special, and numeric count.")

        def calculate_counts(level: str) -> np.ndarray:
            if len(level) == 0:
                return np.array([0, 0, 0, 0])

            full_count = len(level)
            alpha_count = sum(c.isalpha() for c in level) / full_count
            numeric_count = sum(c.isdigit() for c in level) / full_count
            special_count = (
                sum(not c.isalnum() and not c.isspace() for c in level) / full_count
            )

            return np.array([full_count, alpha_count, numeric_count, special_count])

        levels = {
            "fqdn": query,
            "thirdleveldomain": label_parts[0] if len(label_parts) > 2 else "",
            "secondleveldomain": label_parts[1] if len(label_parts) > 1 else "",
        }
        counts = {
            level: calculate_counts(level_value)
            for level, level_value in levels.items()
        }

        logger.debug("Get frequency standard deviation, median, variance, and mean.")
        freq_std = np.std(freq)
        freq_var = np.var(freq)
        freq_median = np.median(freq)
        freq_mean = np.mean(freq)

        logger.debug(
            "Get standard deviation, median, variance, and mean for full, alpha, special, and numeric count."
        )
        stats = {}
        for level, count_array in counts.items():
            stats[f"{level}_std"] = np.std(count_array)
            stats[f"{level}_var"] = np.var(count_array)
            stats[f"{level}_median"] = np.median(count_array)
            stats[f"{level}_mean"] = np.mean(count_array)

        logger.debug("Start entropy calculation")

        def calculate_entropy(s: str) -> float:
            if len(s) == 0:
                return 0
            probabilities = [float(s.count(c)) / len(s) for c in dict.fromkeys(list(s))]
            entropy = -sum(p * math.log(p, 2) for p in probabilities)
            return entropy

        entropy = {level: calculate_entropy(value) for level, value in levels.items()}

        logger.debug("Finished entropy calculation")

        # Final feature aggregation as a NumPy array
        basic_features = np.array([label_length, label_max, label_average])
        freq_features = np.array([freq_std, freq_var, freq_median, freq_mean])

        # Flatten counts and stats for each level into arrays
        level_features = np.hstack([counts[level] for level in levels.keys()])
        stats_features = np.array(
            [stats[f"{level}_std"] for level in levels.keys()]
            + [stats[f"{level}_var"] for level in levels.keys()]
            + [stats[f"{level}_median"] for level in levels.keys()]
            + [stats[f"{level}_mean"] for level in levels.keys()]
        )

        # Entropy features
        entropy_features = np.array([entropy[level] for level in levels.keys()])

        # Concatenate all features into a single numpy array
        all_features = np.concatenate(
            [
                basic_features,
                freq,
                freq_features,
                level_features,
                stats_features,
                entropy_features,
            ]
        )

        logger.debug("Finished data transformation")

        return all_features.reshape(1, -1)

    def detect(self) -> None:  # pragma: no cover
        logger.info("Start detecting malicious requests.")
        for message in self.messages:
            # TODO predict all messages
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
        logger.info("Store alert to file.")
        if len(self.warnings) > 0:
            overall_score = median(
                [warning["probability"] for warning in self.warnings]
            )
            alert = {"overall_score": overall_score, "result": self.warnings}
            logger.info(f"Add alert: {alert}")
            with open(os.path.join(tempfile.gettempdir(), "warnings.json"), "a+") as f:
                json.dump(alert, f)
                f.write("\n")
        else:
            logger.info("No warning produced.")


def main(one_iteration: bool = False):  # pragma: no cover
    """
    Creates the :class:`Detector` instance. Starts a loop that continously fetches data.

    Args:
        one_iteration (bool): For testing purposes: stops loop after one iteration

    Raises:
        KeyboardInterrupt: Execution interrupted by user. Closes down the :class:`LogCollector` instance.
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
