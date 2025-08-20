import unittest
import uuid
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, AsyncMock

import marshmallow_dataclass
import numpy as np
from streamad.model import ZScoreDetector, RShashDetector

from src.base.kafka_handler import (
    KafkaMessageFetchException,
)
from src.base.data_classes.batch import Batch
from src.inspector.inspector import InspectorBase, main
# use no_inspector for testing, as it has almost 0 domain logic
from src.inspector.plugins.no_inspector import NoInspector

DEFAULT_DATA = {
    "src_ip": "192.168.0.167",
    "dns_ip": "10.10.0.10",
    "response_ip": "252.79.173.222",
    "timestamp": "",
    "status": "NXDOMAIN",
    "host_domain_name": "24sata.info",
    "record_type": "A",
    "size": "100b",
}

MINIMAL_NO_INSPECTOR_CONFIG = {
    "name": "test_inspector",
    "inspector_class_name": "NoInspector"
}

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def get_batch(data):
    begin = datetime.now()
    end = begin + timedelta(0, 3)
    test_batch = Batch(
        batch_tree_row_id=f"{uuid.uuid4()}-{uuid.uuid4()}",
        batch_id=uuid.uuid4(),
        begin_timestamp=begin,
        end_timestamp=end,
        data=data if data is not None else [],
    )
    return test_batch


class TestInit(unittest.TestCase):
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    def test_init(
        self, mock_kafka_consume_handler, mock_produce_handler, mock_clickhouse
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = NoInspector(
            consume_topic="test_topic",
            produce_topics=["produce_topic_1"],
            config=MINIMAL_NO_INSPECTOR_CONFIG
        )

        self.assertEqual([], sut.messages)
        self.assertEqual(mock_kafka_consume_handler_instance, sut.kafka_consume_handler)
        mock_kafka_consume_handler.assert_called_once_with("test_topic")


class TestGetData(unittest.TestCase):
    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_get_data_without_return_data(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            None,
            get_batch(None),
        )
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut =  NoInspector(
            consume_topic="test_topic",
            produce_topics=["produce_topic_1"],
            config=MINIMAL_NO_INSPECTOR_CONFIG
        )

        sut.get_and_fill_data()

        self.assertEqual([], sut.messages)

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_get_data_with_return_data(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        test_batch = get_batch([{"test": "test_message_1"}, {"test": "test_message_2"}])
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            None,
            test_batch,
        )
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut =  NoInspector(
            consume_topic="test_topic",
            produce_topics=["produce_topic_1"],
            config=MINIMAL_NO_INSPECTOR_CONFIG
        )

        sut.messages = []
        sut.get_and_fill_data()

        self.assertEqual(test_batch.begin_timestamp, sut.begin_timestamp)
        self.assertEqual(test_batch.end_timestamp, sut.end_timestamp)
        self.assertEqual(
            [{"test": "test_message_1"}, {"test": "test_message_2"}], sut.messages
        )

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_get_data_with_no_return_data(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        begin = None
        end = None
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            None,
            None,
        )
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut =  NoInspector(
            consume_topic="test_topic",
            produce_topics=["produce_topic_1"],
            config=MINIMAL_NO_INSPECTOR_CONFIG
        )

        sut.parent_row_id = f"{uuid.uuid4()}-{uuid.uuid4()}"
        sut.messages = []
        sut.get_and_fill_data()

        self.assertEqual(begin, sut.begin_timestamp)
        self.assertEqual(end, sut.end_timestamp)
        self.assertEqual([], sut.messages)

    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    def test_get_data_while_busy(
        self, mock_kafka_consume_handler, mock_produce_handler, mock_clickhouse
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_and_return_json_data.return_value = "172.126.0.0/24", {
            "begin_timestamp": "2024-05-21T08:31:27.000Z",
            "end_timestamp": "2024-05-21T08:31:29.000Z",
            "data": [
                "test_message_1",
                "test_message_2",
            ],
        }
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut =  NoInspector(
            consume_topic="test_topic",
            produce_topics=["produce_topic_1"],
            config=MINIMAL_NO_INSPECTOR_CONFIG
        )

        sut.messages = ["test_data"]
        sut.get_and_fill_data()

        self.assertEqual(["test_data"], sut.messages)


class TestClearData(unittest.TestCase):
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    def test_clear_data_without_existing_data(
        self, mock_kafka_consume_handler, mock_produce_handler, mock_clickhouse
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_and_return_json_data.return_value = "172.126.0.0/24", {
            "begin_timestamp": "2024-05-21T08:31:27.000Z",
            "end_timestamp": "2024-05-21T08:31:29.000Z",
            "data": [],
        }
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut =  NoInspector(
            consume_topic="test_topic",
            produce_topics=["produce_topic_1"],
            config=MINIMAL_NO_INSPECTOR_CONFIG
        )

        sut.messages = []
        sut.clear_data()

        self.assertEqual([], sut.messages)

    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    def test_clear_data_with_existing_data(
        self, mock_kafka_consume_handler, mock_kafka_produce_handler, mock_clickhouse
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_and_return_json_data.return_value = "172.126.0.0/24", {
            "begin_timestamp": "2024-05-21T08:31:27.000Z",
            "end_timestamp": "2024-05-21T08:31:29.000Z",
            "data": [],
        }
        mock_produce_handler_instance = MagicMock()
        mock_kafka_produce_handler.return_value = mock_produce_handler_instance

        sut =  NoInspector(
            consume_topic="test_topic",
            produce_topics=["produce_topic_1"],
            config=MINIMAL_NO_INSPECTOR_CONFIG
        )

        sut.messages = ["test_data"]
        sut.begin_timestamp = "2024-05-21T08:31:27.000Z"
        sut.end_timestamp = "2024-05-21T08:31:29.000Z"
        sut.clear_data()

        self.assertEqual([], sut.messages)
        self.assertIsNone(sut.begin_timestamp)
        self.assertIsNone(sut.end_timestamp)

class TestSend(unittest.TestCase):
    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_send(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_kafka_produce_handler,
        mock_logger,
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_produce_handler_instance = MagicMock()
        mock_kafka_produce_handler.return_value = mock_produce_handler_instance
        batch_schema = marshmallow_dataclass.class_schema(Batch)()

        sut =  NoInspector(
            consume_topic="test_topic",
            produce_topics=["pipeline-inspector_to_detector"],
            config=MINIMAL_NO_INSPECTOR_CONFIG
        )

        sut.anomalies = [0.9, 0.9]
        sut.X = np.array([[0.0], [0.0]])
        sut.parent_row_id = f"{uuid.uuid4()}-{uuid.uuid4()}"
        sut.begin_timestamp = datetime.now()
        sut.end_timestamp = datetime.now() + timedelta(0, 0, 2)
        data = DEFAULT_DATA
        data["timestamp"] = datetime.strftime(
            sut.begin_timestamp + timedelta(0, 0, 1), TIMESTAMP_FORMAT
        )
        sut.messages = [data]
        mock_batch_tree_row_id = f"{uuid.UUID('754a64f3-a461-4e7b-b4cb-ab29df9c4dce')}-{uuid.UUID('f9b3cbb7-b26c-41be-8e7f-a69a9c133668')}"
        mock_batch_id = uuid.UUID("5ae0872e-5bb9-472c-8c37-8c173213a51f")
        with patch("src.inspector.inspector.uuid") as mock_uuid:
            with patch("src.inspector.inspector.generate_collisions_resistant_uuid") as mock_row_id:
                mock_row_id.return_value = mock_batch_tree_row_id
                mock_uuid.uuid4.return_value = mock_batch_id
                sut.send_data()

        mock_produce_handler_instance.produce.assert_called_once_with(
            topic="pipeline-inspector_to_detector",
            data=batch_schema.dumps(
                {   
                    "batch_tree_row_id": mock_batch_tree_row_id,
                    "batch_id": mock_batch_id,
                    "begin_timestamp": sut.begin_timestamp,
                    "end_timestamp": sut.end_timestamp,
                    "data": [data],
                }
            ),
            key="192.168.0.167",
        )

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_send_not_suspicious(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance
        batch_schema = marshmallow_dataclass.class_schema(Batch)()

        sut =  NoInspector(
            consume_topic="test_topic",
            produce_topics=["produce_topic_1"],
            config=MINIMAL_NO_INSPECTOR_CONFIG
        )

        mock_is_subnet_suspicious = MagicMock(return_value=False)
        sut.subnet_is_suspicious = mock_is_subnet_suspicious
        sut.anomalies = [0.0, 0.0]
        sut.X = np.array([[0.0], [0.0]])
        sut.parent_row_id = f"{uuid.uuid4()}-{uuid.uuid4()}"
        sut.begin_timestamp = datetime.now()
        sut.end_timestamp = datetime.now() + timedelta(0, 0, 2)
        data = DEFAULT_DATA
        data["timestamp"] = datetime.strftime(
            sut.begin_timestamp + timedelta(0, 0, 1), TIMESTAMP_FORMAT
        )
        data["logline_id"] = str(uuid.UUID("99a427a6-ba3f-4aa2-b848-210d994d9108"))
        sut.messages = [data]
        mock_batch_id = uuid.UUID("5ae0872e-5bb9-472c-8c37-8c173213a51f")
        with patch("src.inspector.inspector.uuid") as mock_uuid:
            mock_uuid.uuid4.return_value = mock_batch_id
            sut.send_data()

        mock_produce_handler_instance.produce.assert_not_called()



class TestInspector(InspectorBase):
    def __init__(self, consume_topic, produce_topics, config) -> None:
        super().__init__(consume_topic, produce_topics, config)
        self.inspected = False
        self.anomalies_detected = False
    
    def _get_models(self, models) -> list:
        return ["mock_model"]
    
    def inspect_anomalies(self) -> None:
        self.inspected = True
        if self.messages:
            self.anomalies_detected = True
    
    def subnet_is_suspicious(self) -> bool:
        return self.anomalies_detected

class TestInspectMethod(unittest.TestCase):
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    def test_inspect_no_models_configured(
        self,
        mock_consume_handler,
        mock_produce_handler,
        mock_clickhouse,
    ):
        """Tests that inspect() raises NotImplementedError when no models are configured."""
        # Arrange

        config = MINIMAL_NO_INSPECTOR_CONFIG.copy()
        config.update({
            "inspector_class_name": "TestInspector",
            "mode": "test_mode",
            "anomaly_threshold": 0.5,
            "score_threshold": 0.7,
            "time_type": "test_time",
            "time_range": "test_range"
        })
        sut = TestInspector(
            consume_topic="test_topic",
            produce_topics=["produce_topic_1"],
            config=config
        )
        sut.messages = [{"test": "data"}]
        
        # Assert
        with self.assertRaises(NotImplementedError):
            sut.inspect()
    
    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    def test_inspect_multiple_models_configured(
        self,
        mock_consume_handler,
        mock_produce_handler,
        mock_clickhouse,
        mock_logger,
    ):
        """Tests that inspect() uses only the first model when multiple models are configured."""
        # Setup
        config = MINIMAL_NO_INSPECTOR_CONFIG.copy()
        config.update({
            "inspector_class_name": "TestInspector",
            "mode": "test_mode",
            "models": [
                {"model": "Model1"},
                {"model": "Model2"}
            ],
            "anomaly_threshold": 0.5,
            "score_threshold": 0.7,
            "time_type": "test_time",
            "time_range": "test_range"
        })
        
        sut = TestInspector(
            consume_topic="test_topic",
            produce_topics=["produce_topic_1"],
            config=config
        )
        
        # Mock data
        sut.messages = [{"test": "data"}]
        
        # Execute
        sut.inspect()
        
        # Verify
        self.assertTrue(sut.inspected)
        mock_logger.warning.assert_called_with(
            "Model List longer than 1. Only the first one is taken: Model1!"
        )
    
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    def test_inspect_single_model_configured(
        self,
        mock_consume_handler,
        mock_produce_handler,
        mock_clickhouse,
    ):
        """Tests that inspect() works correctly with a single model configured."""
        # Setup
        config = MINIMAL_NO_INSPECTOR_CONFIG.copy()
        config.update({
            "inspector_class_name": "TestInspector",

            "mode": "test_mode",
            "models": [
                {"model": "Model1"}
            ],
            "anomaly_threshold": 0.5,
            "score_threshold": 0.7,
            "time_type": "test_time",
            "time_range": "test_range"
        })
        
        sut = TestInspector(
            consume_topic="test_topic",
            produce_topics=["produce_topic_1"],
            config=config
        )
        
        # Mock data
        sut.messages = [{"test": "data"}]
        
        # Execute
        sut.inspect()
        
        # Verify
        self.assertTrue(sut.inspected)


class TestBootStrapFunction(unittest.TestCase):
    
    def setUp(self):
        self.inspectors = [
            {
                "name": "test_inspector",
                "inspector_class_name": "NoInspector",
                "prefilter_name": "dominator_filter",
                "inspector_module_name": "no_inspector"
            }
        ]
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.logger")
    def test_bootstrap_normal_execution(self, mock_logger,mock_consume_handler, mock_produce_handler, mock_clickhouse):
        """Tests that the bootstrap process executes all steps in the correct order."""
        # Setup
        config = MINIMAL_NO_INSPECTOR_CONFIG.copy()
        config.update({
            "inspector_class_name": "TestInspector",
            "mode": "test_mode",
            "models": [{"model": "ZScoreDetector"}],
            "anomaly_threshold": 0.5,
            "score_threshold": 0.7,
            "time_type": "test_time",
            "time_range": "test_range"
        })
        
        sut = TestInspector(
            consume_topic="test_topic",
            produce_topics=["produce_topic_1"],
            config=config
        )
        
        # Mock data so send_data works
        sut.messages = [{"src_ip": "192.168.0.1", "logline_id": "test_id"}]
        sut.parent_row_id = f"{uuid.uuid4()}-{uuid.uuid4()}"
        sut.begin_timestamp = datetime.now()
        sut.end_timestamp = datetime.now() + timedelta(seconds=1)
        
        # Patch methods to control the loop
        original_send_data = sut.send_data
        def mock_send_data():
            original_send_data()
            # After first iteration, raise exception to break the loop
            raise StopIteration("Test exception to break loop")
        
        sut.send_data = mock_send_data
        # Track method calls
        sut.inspect = MagicMock(wraps=sut.inspect)
        sut.send_data = MagicMock(wraps=sut.send_data)
        sut.clear_data = MagicMock(wraps=sut.clear_data)
        
        # Execute and verify
        with self.assertRaises(StopIteration):
            sut.bootstrap_inspection_process()
        
        # Verify method call order and count
        sut.inspect.assert_called_once()
        sut.send_data.assert_called_once()
        sut.clear_data.assert_called_once()
        
        # Verify logger messages
        mock_logger.info.assert_any_call("Starting test_inspector")
        

    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.logger")
    def test_bootstrap_kafka_exception_handling(self, mock_logger, mock_consume_handler, mock_produce_handler, mock_clickhouse):
        """Tests that IOError is handled correctly (re-raised)."""
        # Setup
        config = MINIMAL_NO_INSPECTOR_CONFIG.copy()
        config.update({
            "inspector_class_name": "TestInspector",
            "mode": "test_mode",
            "models": [{"model": "ZScoreDetector"}],
            "anomaly_threshold": 0.5,
            "score_threshold": 0.7,
            "time_type": "test_time",
            "time_range": "test_range"
        })
        
        sut = TestInspector(
            consume_topic="test_topic",
            produce_topics=["produce_topic_1"],
            config=config
        )
        
        # Mock data so send_data works
        sut.messages = [{"src_ip": "192.168.0.1", "logline_id": "test_id"}]
        sut.parent_row_id = f"{uuid.uuid4()}-{uuid.uuid4()}"
        sut.begin_timestamp = datetime.now()
        sut.end_timestamp = datetime.now() + timedelta(seconds=1)

        # Patch inspect to raise IOError
        def mock_inspect():
            raise IOError("Test IO error")
        
        sut.inspect = mock_inspect
        sut.get_and_fill_data = MagicMock()
        sut.send_data = MagicMock()
        sut.clear_data = MagicMock(wraps=sut.clear_data)
        
        # Execute and verify
        with self.assertRaises(IOError) as context:
            sut.bootstrap_inspection_process()
            self.assertEqual(str(context.exception), "Test IO error")
        
        
        # Verify method calls
        sut.get_and_fill_data.assert_called_once()
        sut.send_data.assert_not_called()
        sut.clear_data.assert_called_once()
class TestMainFunction(unittest.IsolatedAsyncioTestCase):
    
    def setUp(self):
        self.inspectors = [
            {
                "name": "test_inspector",
                "inspector_class_name": "NoInspector",
                "prefilter_name": "dominator_filter",
                "inspector_module_name": "no_inspector"
            }
        ]
    
    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.plugins.no_inspector.NoInspector")
    @patch("asyncio.create_task")  
    @patch("asyncio.run")
    async def test_main_succesful_start(self,mock_asyncio_run, mock_asyncio_create_task, mock_inspector, mock_logger):
        # Arrange
        mock_inspector_instance = MagicMock()
        mock_inspector_instance.start = AsyncMock()
        mock_inspector.return_value = mock_inspector_instance
        mock_asyncio_create_task.side_effect = lambda coro: coro

        # Act
        with patch("src.inspector.inspector.INSPECTORS", self.inspectors):
            await main()

        # Assert
        mock_inspector_instance.start.assert_called_once()


if __name__ == "__main__":
    unittest.main()
