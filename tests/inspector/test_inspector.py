import unittest
import uuid
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, AsyncMock

import marshmallow_dataclass
import numpy as np
from streamad.model import ZScoreDetector, RShashDetector

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
    
    # TODO: test as bootstrap 
    # @patch("src.inspector.inspector.logger")
    # @patch("src.inspector.inspector.InspectorBase")
    # def test_main_loop_execution(self, mock_inspector, mock_logger):
    #     # Arrange
    #     mock_inspector_instance = mock_inspector.return_value

    #     mock_inspector_instance.get_and_fill_data = MagicMock()
    #     mock_inspector_instance.clear_data = MagicMock()

    #     # Act
    #     main()

    #     # Assert
    #     self.assertTrue(mock_inspector_instance.get_and_fill_data.called)
    #     self.assertTrue(mock_inspector_instance.clear_data.called)

    # TODO: TEST ERRORS IN BOOTSTRAP METHOD
    # @patch("src.inspector.inspector.logger")
    # @patch("src.inspector.inspector.InspectorBase")
    # def test_main_io_error_handling(self, mock_inspector, mock_logger):
    #     # Arrange
    #     mock_inspector_instance = mock_inspector.return_value

    #     # Act
    #     with patch.object(
    #         mock_inspector_instance,
    #         "get_and_fill_data",
    #         side_effect=IOError("Simulated IOError"),
    #     ):
    #         with self.assertRaises(IOError):
    #             main()

    #     # Assert
    #     self.assertTrue(mock_inspector_instance.clear_data.called)
    #     self.assertTrue(mock_inspector_instance.loop_exited)

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


    # @patch("src.inspector.inspector.logger")
    # @patch("src.inspector.inspector.Inspector")
    # def test_main_keyboard_interrupt(self, mock_inspector, mock_logger):
    #     # Arrange
    #     mock_inspector_instance = mock_inspector.return_value
    #     mock_inspector_instance.get_and_fill_data.side_effect = KeyboardInterrupt

    #     # Act
    #     main()

    #     # Assert
    #     self.assertTrue(mock_inspector_instance.clear_data.called)
    #     self.assertTrue(mock_inspector_instance.loop_exited)


if __name__ == "__main__":
    unittest.main()
