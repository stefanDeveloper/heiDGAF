import importlib
import os
import sys
import uuid
from datetime import datetime
from enum import Enum, unique
import asyncio
from abc import ABC, abstractmethod
import marshmallow_dataclass
import numpy as np
from streamad.util import StreamGenerator, CustomDS
sys.path.append(os.getcwd())
from src.base.clickhouse_kafka_sender import ClickHouseKafkaSender
from src.base.data_classes.batch import Batch
from src.base.utils import (
    setup_config,
    get_zeek_sensor_topic_base_names,
    generate_collisions_resistant_uuid,
)
from src.base.kafka_handler import (
    ExactlyOnceKafkaConsumeHandler,
    ExactlyOnceKafkaProduceHandler,
    KafkaMessageFetchException,
)
from src.base.log_config import get_logger

module_name = "data_inspection.inspector"
logger = get_logger(module_name)

config = setup_config()
PRODUCE_TOPIC_PREFIX = config["environment"]["kafka_topics_prefix"]["pipeline"][
    "inspector_to_detector"
]
CONSUME_TOPIC_PREFIX = config["environment"]["kafka_topics_prefix"]["pipeline"][
    "prefilter_to_inspector"
]
SENSOR_PROTOCOLS = get_zeek_sensor_topic_base_names(config)
PREFILTERS = config["pipeline"]["log_filtering"]
INSPECTORS = config["pipeline"]["data_inspection"]
COLLECTORS = config["pipeline"]["log_collection"]["collectors"]
DETECTORS = config["pipeline"]["data_analysis"]
PLUGIN_PATH = "src.inspector.plugins"
KAFKA_BROKERS = ",".join(
    [
        f"{broker['hostname']}:{broker['port']}"
        for broker in config["environment"]["kafka_brokers"]
    ]
)
class InspectorAbstractBase(ABC): # pragma: no cover
    @abstractmethod
    def __init__(self, consume_topic, produce_topics, config) -> None:
        pass
    @abstractmethod
    def inspect_anomalies(self) -> None:
        pass
    @abstractmethod
    def _get_models(self, models) -> list:
        pass
    @abstractmethod
    def subnet_is_suspicious(self) -> bool: 
        pass
         
class InspectorBase(InspectorAbstractBase):
    """Finds anomalies in a batch of requests and produces it to the ``Detector``."""

    def __init__(self, consume_topic, produce_topics, config) -> None:
        """
        Initializes the InspectorBase with necessary configurations and connections.
        
        Sets up Kafka handlers, database connections, and configuration parameters based on
        the provided configuration. For non-NoInspector implementations, initializes model
        related parameters including mode, model configurations, thresholds, and time parameters.
        
        Args:
            consume_topic (str): Kafka topic to consume messages from
            produce_topics (list): List of Kafka topics to produce messages to
            config (dict): Configuration dictionary containing inspector settings
            
        Note:
            The "NoInspector" implementation skips model configuration initialization
            as it doesn't perform actual anomaly detection.
        """

        if not config["inspector_class_name"] == "NoInspector":
            self.mode = config["mode"]
            self.model_configurations = config["models"] if "models" in config.keys() else None
            self.anomaly_threshold = config["anomaly_threshold"]
            self.score_threshold = config["score_threshold"]
            self.time_type = config["time_type"]
            self.time_range = config["time_range"]
        self.name = config["name"]
        self.consume_topic = consume_topic
        self.produce_topics = produce_topics
        self.batch_id = None
        self.X = None
        self.key = None
        self.begin_timestamp = None
        self.end_timestamp = None

        self.messages = []
        self.anomalies = []

        self.kafka_consume_handler = ExactlyOnceKafkaConsumeHandler(self.consume_topic)
        self.kafka_produce_handler = ExactlyOnceKafkaProduceHandler()

        # databases
        self.batch_tree = ClickHouseKafkaSender("batch_tree")
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
        if data:
            self.parent_row_id = data.batch_tree_row_id
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
                instance_name=self.name,
                timestamp=datetime.now(),
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
                timestamp=datetime.now(),
                parent_batch_row_id=self.parent_row_id,
                batch_id=self.batch_id,
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

    def clear_data(self):
        """Clears the data in the internal data structures."""
        self.messages = []
        self.anomalies = []
        self.X = []
        self.begin_timestamp = None
        self.end_timestamp = None
        logger.debug("Cleared messages and timestamps. Inspector is now available.")

    def send_data(self):
        """Pass the anomalous data for the detector unit for further processing"""
        row_id = generate_collisions_resistant_uuid()
        if self.subnet_is_suspicious():            
            buckets = {}
            for message in self.messages:
                if message["src_ip"] in buckets.keys():
                    buckets[message["src_ip"]].append(message)
                else:
                    buckets[message["src_ip"]] = []
                    buckets.get(message["src_ip"]).append(message)

            for key, value in buckets.items():

                suspicious_batch_id = uuid.uuid4()  # generate new suspicious_batch_id

                self.suspicious_batches_to_batch.insert(
                    dict(
                        suspicious_batch_id=suspicious_batch_id,
                        batch_id=self.batch_id,
                    )
                )

                data_to_send = {
                    "batch_tree_row_id": row_id,
                    "batch_id": suspicious_batch_id,
                    "begin_timestamp": self.begin_timestamp,
                    "end_timestamp": self.end_timestamp,
                    "data": value,
                }

                batch_schema = marshmallow_dataclass.class_schema(Batch)()

                # important to finish before sending, otherwise detector can process before finished here!
                self.suspicious_batch_timestamps.insert(
                    dict(
                        suspicious_batch_id=suspicious_batch_id,
                        src_ip=key,
                        stage=module_name,
                        instance_name=self.name,
                        status="finished",
                        timestamp=datetime.now(),
                        is_active=True,
                        message_count=len(value),
                    )
                )

                self.batch_tree.insert(
                    dict(
                        batch_row_id=row_id,
                        stage=module_name,
                        instance_name=self.name,
                        status="finished",
                        timestamp=datetime.now(),
                        parent_batch_row_id=self.parent_row_id,
                        batch_id=suspicious_batch_id,
                    )
                )
                for topic in self.produce_topics:
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
                    instance_name=self.name,
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

            self.batch_tree.insert(
                dict(
                    batch_row_id=row_id,
                    stage=module_name,
                    instance_name=self.name,
                    status="finished",
                    timestamp=datetime.now(),
                    parent_batch_row_id=self.parent_row_id,
                    batch_id=self.batch_id,
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
    def inspect(self):
        """
        Executes the anomaly detection process with validation and fallback handling.
        
        This method:
        1. Validates that model configurations exist
        2. Logs a warning if multiple models are configured (only first is used)
        3. Retrieves the models through _get_models()
        4. Calls inspect_anomalies() to perform the actual detection
        
        Raises:
            NotImplementedError: If no model configurations are provided
        """
        if self.model_configurations == None or len(self.model_configurations) == 0:
            logger.warning("No model ist set!")
            raise NotImplementedError(f"No model is set!")
        if len(self.model_configurations) > 1:
            logger.warning(
                f"Model List longer than 1. Only the first one is taken: {self.model_configurations[0]['model']}!"
            )        
        self.models = self._get_models(self.model_configurations)
        self.inspect_anomalies()   
            
    # TODO: test this!
    def bootstrap_inspection_process(self):
        """
        Implements the main inspection process loop that continuously:
        1. Fetches new data from Kafka
        2. Inspects the data for anomalies
        3. Sends suspicious data to detectors
        
        The loop handles various exceptions:
        - KafkaMessageFetchException: Logged as debug (transient issue)
        - IOError: Logged as error and re-raised (critical failure)
        - ValueError: Logged as debug (data validation issue)
        - KeyboardInterrupt: Gracefully shuts down the inspector
        """
        logger.info(f"Starting {self.name}")
        while True:
            try:
                self.get_and_fill_data()
                self.inspect()
                self.send_data()
            except KafkaMessageFetchException as e:  # pragma: no cover
                logger.debug(e)
            except IOError as e:
                logger.error(e)
                raise e
            except ValueError as e:
                logger.debug(e)
            except KeyboardInterrupt:
                logger.info(f" {self.consume_topic}  Closing down Inspector...")
                break
            finally:
                self.clear_data()

    async def start(self): # pragma: no cover
        """
        Starts the inspector in an asynchronous context.
        
        This method runs the synchronous bootstrap_inspection_process() in a separate
        thread using run_in_executor, allowing the inspector to operate concurrently
        with other async components in the pipeline.
        """
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.bootstrap_inspection_process)


async def main():
    """
    Entry point for the Inspector module.
    
    This function:
    1. Iterates through all configured inspectors
    2. Creates the appropriate inspector instance based on configuration
    3. Starts each inspector as an asynchronous task
    4. Gathers all tasks to run them concurrently
    
    The function dynamically loads inspector classes from the plugin system
    based on configuration values, allowing for flexible extension of the
    inspection capabilities.
    
    """
    tasks = []
    for inspector in INSPECTORS:
        logger.info(inspector["name"])
        consume_topic = f"{CONSUME_TOPIC_PREFIX}-{inspector['name']}"
        produce_topics = [
            f"{PRODUCE_TOPIC_PREFIX}-{detector['name']}"
            for detector in DETECTORS
            if detector["inspector_name"] == inspector["name"]
        ]
        class_name = inspector["inspector_class_name"]
        module_name = f"{PLUGIN_PATH}.{inspector['inspector_module_name']}"
        module = importlib.import_module(module_name)
        InspectorClass = getattr(module, class_name)
        logger.info(f"using {class_name} and {module_name}")
        inspector_instance = InspectorClass(consume_topic=consume_topic, produce_topics=produce_topics, config=inspector)
        tasks.append(asyncio.create_task(inspector_instance.start()))
    await asyncio.gather(*tasks)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
