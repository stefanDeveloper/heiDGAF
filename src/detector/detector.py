import datetime
import hashlib
import json
import os
import pickle
import sys
import tempfile
import asyncio
import numpy as np
import requests
from numpy import median
from abc import ABC, abstractmethod
import importlib

sys.path.append(os.getcwd())
from src.base.clickhouse_kafka_sender import ClickHouseKafkaSender
from src.base.utils import setup_config, generate_collisions_resistant_uuid
from src.base.kafka_handler import (
    ExactlyOnceKafkaConsumeHandler,
    KafkaMessageFetchException,
)
from src.base.log_config import get_logger

module_name = "data_analysis.detector"
logger = get_logger(module_name)

BUF_SIZE = 65536  # let's read stuff in 64kb chunks!

config = setup_config()
INSPECTORS = config["pipeline"]["data_inspection"]
DETECTORS = config["pipeline"]["data_analysis"]


CONSUME_TOPIC_PREFIX = config["environment"]["kafka_topics_prefix"]["pipeline"][
    "inspector_to_detector"
]

PLUGIN_PATH = "src.detector.plugins"
class WrongChecksum(Exception):  # pragma: no cover
    """
    Exception if Checksum is not equal.
    """

    pass


class DetectorAbstractBase(ABC):    # pragma: no cover
    """
    Abstract base class for all detector implementations.

    This class defines the interface that all concrete detector implementations must follow.
    It provides the essential methods that need to be implemented for a detector to function
    within the pipeline.

    Subclasses must implement all abstract methods to ensure proper integration with the
    detection system.
    """
    @abstractmethod
    def __init__(self, detector_config, consume_topic) -> None:
        pass
    @abstractmethod
    def get_model_download_url(self):
        pass
    @abstractmethod
    def get_scaler_download_url(self):
        pass     
    @abstractmethod
    def predict(self, message) -> np.ndarray :
        pass


class DetectorBase(DetectorAbstractBase):
    """
    Base implementation for detectors in the pipeline.

    This class provides a concrete implementation of the detector interface with
    common functionality shared across all detector types. It handles model
    management, data processing, Kafka communication, and result reporting.

    The class is designed to be extended by specific detector implementations
    that provide model-specific prediction logic.
    """

    def __init__(self, detector_config, consume_topic) -> None:
        """
        Initialize the detector with configuration and Kafka topic settings.

        Sets up all necessary components including model loading, Kafka handlers,
        and database connections.

        Args:
            detector_config (dict): Configuration dictionary containing detector-specific
                parameters such as name, model, checksum, and threshold.
            consume_topic (str): Kafka topic from which the detector will consume messages.
        """
        
        self.name = detector_config["name"]
        self.model = detector_config["model"]
        self.checksum = detector_config["checksum"]
        self.threshold = detector_config["threshold"]

        self.consume_topic = consume_topic
        self.suspicious_batch_id = None
        self.key = None
        self.messages = []
        self.warnings = []
        self.begin_timestamp = None
        self.end_timestamp = None
        self.model_path = os.path.join(
            tempfile.gettempdir(), f"{self.model}_{self.checksum}_model.pickle"
        )
        self.scaler_path = os.path.join(
            tempfile.gettempdir(), f"{self.model}_{self.checksum}_scaler.pickle"
        )

        self.kafka_consume_handler = ExactlyOnceKafkaConsumeHandler(self.consume_topic)

        self.model, self.scaler = self._get_model()

        # databases
        self.batch_tree = ClickHouseKafkaSender("batch_tree")
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
        """
        Consume data from Kafka and store it for processing.

        This method retrieves messages from the Kafka topic, processes them, and
        prepares the data for detection. It handles batch management, timestamp
        tracking, and database updates for monitoring purposes.

        The method also manages the flow of data through the pipeline by updating
        relevant database tables with processing status and metrics.
        """
        if self.messages:
            logger.warning(
                "Detector is busy: Not consuming new messages. Wait for the Detector to finish the "
                "current workload."
            )
            return
        key, data = self.kafka_consume_handler.consume_as_object()
        if data.data:
            self.parent_row_id = data.batch_tree_row_id
            self.suspicious_batch_id = data.batch_id
            self.begin_timestamp = data.begin_timestamp
            self.end_timestamp = data.end_timestamp
            self.messages = data.data
            self.key = key
        self.suspicious_batch_timestamps.insert(
            dict(
                suspicious_batch_id=self.suspicious_batch_id,
                src_ip=key,
                stage=module_name,
                instance_name=self.name,
                status="in_process",
                timestamp=datetime.datetime.now(),
                is_active=True,
                message_count=len(self.messages),
            )
        )
        row_id = generate_collisions_resistant_uuid()

        self.batch_tree.insert(
            dict(
                batch_row_id=row_id,
                stage=module_name,
                instance_name=self.name,
                status="in_process",
                timestamp=datetime.datetime.now(),
                parent_batch_row_id=self.parent_row_id,
                batch_id=self.suspicious_batch_id,
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
        """
        Calculate the SHA256 checksum of a file.

        This utility method reads a file in chunks and computes its SHA256 hash,
        which is used for model integrity verification.

        Args:
            file_path (str): Path to the file for which the checksum should be calculated.

        Returns:
            str: Hexadecimal string representation of the SHA256 checksum.
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
        """
        Download and validate the detection model.

        This method handles the model management process:
        1. Checks if the model already exists locally
        2. Downloads the model if not present
        3. Verifies the model's integrity using SHA256 checksum
        4. Loads the model for use in detection

        The method ensures that only verified models are used for detection to
        maintain system reliability.

        Returns:
            object: The loaded model object ready for prediction.

        Raises:
            WrongChecksum: If the downloaded model's checksum doesn't match the expected value.
            requests.HTTPError: If there's an error downloading the model.
        """
        logger.info(f"Get model: {self.model} with checksum {self.checksum}")
        # TODO test the if!
        if not os.path.isfile(self.model_path):
            model_download_url = self.get_model_download_url()
            logger.info(f"downloading model {self.model} from {model_download_url} with checksum {self.checksum}")
            response = requests.get(model_download_url)
            response.raise_for_status()
            with open(self.model_path, "wb") as f:
                f.write(response.content)
            scaler_download_url = self.get_scaler_download_url()
            scaler_response = requests.get(scaler_download_url)
            scaler_response.raise_for_status()
            with open(self.scaler_path, "wb") as f:
                f.write(scaler_response.content)
                
        # Check file sha256
        local_checksum = self._sha256sum(self.model_path)

        if local_checksum != self.checksum:
            logger.warning(
                f"Checksum {self.checksum} SHA256 is not equal with new checksum {local_checksum}!"
            )
            raise WrongChecksum(
                f"Checksum {self.checksum} SHA256 is not equal with new checksum {local_checksum}!"
            )

        with open(self.model_path, "rb") as input_file:
            clf = pickle.load(input_file)

        with open(self.scaler_path, "rb") as input_file:
            scaler = pickle.load(input_file)

        return clf, scaler

    def detect(self) -> None:
        """
        Process messages to detect malicious requests.

        This method applies the detection model to each message in the current batch,
        identifies potential threats based on the model's predictions, and collects
        warnings for further processing.

        The detection uses a threshold to determine if a prediction indicates
        malicious activity, and only warnings exceeding this threshold are retained.

        Note:
            This method relies on the implementation of ``predict``of the rspective subclass
        """
        logger.info("Start detecting malicious requests.")
        for message in self.messages:
            # TODO predict all messages
            y_pred = self.predict(message)
            logger.info(f"Prediction: {y_pred}")
            if np.argmax(y_pred, axis=1) == 1 and y_pred[0][1] > self.threshold:
                logger.info("Append malicious request to warning.")
                warning = {
                    "request": message,
                    "probability": float(y_pred[0][1]),
                    # TODO: what is the use of this? not even json serializabel ?
                    # "model": self.model,
                    "name": self.name,
                    "sha256": self.checksum,
                }
                self.warnings.append(warning)


    def clear_data(self):
        """Clears the data in the internal data structures."""
        self.messages = []
        self.begin_timestamp = None
        self.end_timestamp = None
        self.warnings = []


    def send_warning(self) -> None:
        """
        Dispatch detected warnings to the appropriate systems.

        This method handles the reporting of detected threats by:
        1. Calculating an overall threat score
        2. Storing detailed warning information
        3. Updating database records with detection results
        4. Marking processed loglines with appropriate status

        The method updates multiple database tables to maintain the pipeline's
        state tracking and provides detailed information about detected threats.
        """
        logger.info("Store alert.")
        row_id = generate_collisions_resistant_uuid()
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
                    src_ip=self.key,
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
                    src_ip=self.key,
                    stage=module_name,
                    instance_name=self.name,
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
                    src_ip=self.key,
                    stage=module_name,
                    instance_name=self.name,
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

        self.batch_tree.insert(
            dict(
                batch_row_id=row_id,
                stage=module_name,
                instance_name=self.name,
                status="finished",
                timestamp=datetime.datetime.now(),
                parent_batch_row_id=self.parent_row_id,
                batch_id=self.suspicious_batch_id,
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

    
    # TODO: test bootstrap!
    def bootstrap_detector_instance(self):
        """
        Main processing loop for the detector instance.

        This method implements the core processing loop that continuously:
        1. Fetches data from Kafka
        2. Performs detection on the data
        3. Sends warnings for detected threats
        4. Handles exceptions and cleanup

        The loop continues until interrupted by a keyboard interrupt (Ctrl+C),
        at which point it performs a graceful shutdown.

        Note:
            This method is designed to run in a dedicated thread or process.
        """
        while True:
            try:
                logger.debug("Before getting and filling data")
                self.get_and_fill_data()
                logger.debug("Inspect Data")
                self.detect()
                logger.debug("Send warnings")
                self.send_warning()
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
                self.clear_data()

    async def start(self): # pragma: no cover
        """
        Start the detector instance asynchronously.

        This method sets up the detector to run in an asynchronous execution context,
        allowing it to operate concurrently with other components in the system.
        """
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.bootstrap_detector_instance)

    
async def main():  # pragma: no cover
    """
    Initialize and start all detector instances defined in the configuration.

    This function:
    1. Reads detector configurations
    2. Dynamically loads detector classes
    3. Creates detector instances
    4. Starts all detectors concurrently
    """
    tasks = []
    for detector_config in DETECTORS:
        consume_topic = f"{CONSUME_TOPIC_PREFIX}-{detector_config['name']}"
        class_name = detector_config["detector_class_name"]
        module_name = f"{PLUGIN_PATH}.{detector_config['detector_module_name']}"
        module = importlib.import_module(module_name)
        DetectorClass = getattr(module, class_name)
        detector = DetectorClass(detector_config=detector_config, consume_topic=consume_topic)
        tasks.append(asyncio.create_task(detector.start()))
    await asyncio.gather(*tasks)

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
