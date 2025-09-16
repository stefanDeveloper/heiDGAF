import unittest
import uuid
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import marshmallow_dataclass
import numpy as np
from streamad.model import ZScoreDetector, RShashDetector

from src.base.data_classes.batch import Batch
from src.inspector.inspector import Inspector, main

DEFAULT_DATA = {
    "client_ip": "192.168.0.167",
    "dns_ip": "10.10.0.10",
    "response_ip": "252.79.173.222",
    "timestamp": "",
    "status": "NXDOMAIN",
    "host_domain_name": "24sata.info",
    "record_type": "A",
    "size": "100b",
}

TIMESTAMP_FORMAT: str = "%Y-%m-%dT%H:%M:%S.%fZ"


def get_batch(data):
    begin = datetime.now()
    end = begin + timedelta(0, 3)
    test_batch = Batch(
        batch_id=uuid.uuid4(),
        begin_timestamp=begin,
        end_timestamp=end,
        data=data if data is not None else [],
    )
    return test_batch


class TestInit(unittest.TestCase):
    @patch("src.inspector.inspector.CONSUME_TOPIC", "test_topic")
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

        sut = Inspector()

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

        sut = Inspector()
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

        sut = Inspector()
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

        sut = Inspector()
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

        sut = Inspector()
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

        sut = Inspector()
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

        sut = Inspector()
        sut.messages = ["test_data"]
        sut.begin_timestamp = "2024-05-21T08:31:27.000Z"
        sut.end_timestamp = "2024-05-21T08:31:29.000Z"
        sut.clear_data()

        self.assertEqual([], sut.messages)
        self.assertIsNone(sut.begin_timestamp)
        self.assertIsNone(sut.end_timestamp)


class TestDataFunction(unittest.TestCase):

    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.TIME_TYPE", "ms")
    @patch("src.inspector.inspector.TIME_RANGE", 1)
    def test_count_errors(
        self, mock_kafka_consume_handler, mock_produce_handler, mock_clickhouse
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Inspector()
        begin_timestamp = datetime.now()
        end_timestamp = datetime.now() + timedelta(0, 0, 2)
        data = DEFAULT_DATA
        data["timestamp"] = datetime.strftime(
            begin_timestamp + timedelta(0, 0, 1), TIMESTAMP_FORMAT
        )
        messages = [data]
        np.testing.assert_array_equal(
            np.asarray([[1.0], [0.0]]),
            sut._count_errors(messages, begin_timestamp, end_timestamp),
        )

    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.TIME_TYPE", "ms")
    @patch("src.inspector.inspector.TIME_RANGE", 1)
    def test_mean_packet_size(
        self, mock_kafka_consume_handler, mock_produce_handler, mock_clickhouse
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Inspector()
        begin_timestamp = datetime.now()
        end_timestamp = datetime.now() + timedelta(0, 0, 2)
        data = DEFAULT_DATA
        data["timestamp"] = datetime.strftime(
            begin_timestamp + timedelta(0, 0, 1), TIMESTAMP_FORMAT
        )
        messages = [data]
        np.testing.assert_array_equal(
            np.asarray([[100], [0.0]]),
            sut._mean_packet_size(messages, begin_timestamp, end_timestamp),
        )

    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    def test_count_errors_empty_messages(
        self, mock_kafka_consume_handler, mock_produce_handler, mock_clickhouse
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Inspector()
        begin_timestamp = datetime.now()
        end_timestamp = datetime.now() + timedelta(0, 0, 2)
        data = DEFAULT_DATA
        data["timestamp"] = datetime.strftime(
            begin_timestamp + timedelta(0, 0, 1), TIMESTAMP_FORMAT
        )
        np.testing.assert_array_equal(
            np.asarray([[0.0], [0.0]]),
            sut._count_errors([], begin_timestamp, end_timestamp),
        )

    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    def test_mean_packet_size_empty_messages(
        self, mock_kafka_consume_handler, mock_produce_handler, mock_clickhouse
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Inspector()
        begin_timestamp = datetime.now()
        end_timestamp = begin_timestamp + timedelta(0, 0, 2)
        data = DEFAULT_DATA
        data["timestamp"] = datetime.strftime(
            begin_timestamp + timedelta(0, 0, 1), TIMESTAMP_FORMAT
        )
        np.testing.assert_array_equal(
            np.asarray([[0.0], [0.0]]),
            sut._mean_packet_size([], begin_timestamp, end_timestamp),
        )


class TestInspectFunction(unittest.TestCase):
    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch(
        "src.inspector.inspector.MODELS",
        None,
    )
    def test_inspect_none_models(
        self,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_clickhouse,
        mock_logger,
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            get_batch(None),
        )
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Inspector()
        with self.assertRaises(NotImplementedError):
            sut.inspect()

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch(
        "src.inspector.inspector.MODELS",
        "",
    )
    def test_inspect_empty_models(
        self,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_clickhouse,
        mock_logger,
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            get_batch(None),
        )
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Inspector()
        with self.assertRaises(NotImplementedError):
            sut.inspect()

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch(
        "src.inspector.inspector.MODELS",
        [{"model": "ZScoreDetector", "module": "streamad.model", "model_args": {}}],
    )
    @patch("src.inspector.inspector.TIME_TYPE", "ms")
    @patch("src.inspector.inspector.TIME_RANGE", 1)
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_inspect_univariate(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        test_batch = get_batch(None)
        test_batch.begin_timestamp = datetime.now()
        test_batch.end_timestamp = datetime.now() + timedelta(0, 0, 2)
        data = DEFAULT_DATA
        data["timestamp"] = datetime.strftime(
            test_batch.begin_timestamp + timedelta(0, 0, 1), TIMESTAMP_FORMAT
        )
        test_batch.data = [data]
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Inspector()
        sut.get_and_fill_data()
        sut.inspect()
        self.assertEqual([0, 0], sut.anomalies)

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch(
        "src.inspector.inspector.MODELS",
        [
            {
                "model": "ZScoreDetector",
                "module": "streamad.model",
                "model_args": {"window_len": 10},
            }
        ],
    )
    @patch("src.inspector.inspector.TIME_TYPE", "ms")
    @patch("src.inspector.inspector.TIME_RANGE", 1)
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_inspect_univariate_2(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        test_batch = get_batch(None)
        test_batch.begin_timestamp = datetime.now()
        test_batch.end_timestamp = datetime.now() + timedelta(0, 0, 2)
        data = DEFAULT_DATA
        data["timestamp"] = datetime.strftime(
            test_batch.begin_timestamp + timedelta(0, 0, 1), TIMESTAMP_FORMAT
        )
        test_batch.data = [data]
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Inspector()
        sut.get_and_fill_data()
        sut.inspect()
        self.assertNotEqual([None, None], sut.anomalies)

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch(
        "src.inspector.inspector.MODELS",
        [
            {"model": "ZScoreDetector", "module": "streamad.model", "model_args": {}},
            {"model": "KNNDetector", "module": "streamad.model", "model_args": {}},
        ],
    )
    @patch("src.inspector.inspector.TIME_TYPE", "ms")
    @patch("src.inspector.inspector.TIME_RANGE", 1)
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_inspect_univariate_two_models(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        test_batch = get_batch(None)
        test_batch.begin_timestamp = datetime.now()
        test_batch.end_timestamp = datetime.now() + timedelta(0, 0, 2)
        data = DEFAULT_DATA
        data["timestamp"] = datetime.strftime(
            test_batch.begin_timestamp + timedelta(0, 0, 1), TIMESTAMP_FORMAT
        )
        test_batch.data = [data]
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Inspector()
        sut.get_and_fill_data()
        sut.inspect()
        self.assertEqual([0, 0], sut.anomalies)
        self.assertTrue(isinstance(sut.model, ZScoreDetector))

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch(
        "src.inspector.inspector.MODELS",
        [{"model": "RShashDetector", "module": "streamad.model", "model_args": {}}],
    )
    @patch("src.inspector.inspector.MODE", "multivariate")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_inspect_multivariate(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        test_batch = get_batch(None)
        test_batch.begin_timestamp = datetime.now()
        test_batch.end_timestamp = datetime.now() + timedelta(0, 0, 2)
        data = DEFAULT_DATA
        data["timestamp"] = datetime.strftime(
            test_batch.begin_timestamp + timedelta(0, 0, 1), TIMESTAMP_FORMAT
        )
        test_batch.data = [data]
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Inspector()
        sut.get_and_fill_data()
        sut.inspect()
        self.assertEqual([0, 0], sut.anomalies)

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch(
        "src.inspector.inspector.MODELS",
        [
            {
                "model": "RShashDetector",
                "module": "streamad.model",
                "model_args": {"window_len": 10},
            }
        ],
    )
    @patch("src.inspector.inspector.MODE", "multivariate")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_inspect_multivariate_window_len(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        test_batch = get_batch(None)
        test_batch.begin_timestamp = datetime.now()
        test_batch.end_timestamp = datetime.now() + timedelta(0, 0, 2)
        data = DEFAULT_DATA
        data["timestamp"] = datetime.strftime(
            test_batch.begin_timestamp + timedelta(0, 0, 1), TIMESTAMP_FORMAT
        )
        test_batch.data = [data]
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Inspector()
        sut.get_and_fill_data()
        sut.inspect()
        self.assertNotEqual([None, None], sut.anomalies)

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch(
        "src.inspector.inspector.MODELS",
        [
            {"model": "RShashDetector", "module": "streamad.model", "model_args": {}},
            {"model": "xStreamDetector", "module": "streamad.model", "model_args": {}},
        ],
    )
    @patch("src.inspector.inspector.MODE", "multivariate")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_inspect_multivariate_two_models(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        test_batch = get_batch(None)
        test_batch.begin_timestamp = datetime.now()
        test_batch.end_timestamp = datetime.now() + timedelta(0, 0, 2)
        data = DEFAULT_DATA
        data["timestamp"] = datetime.strftime(
            test_batch.begin_timestamp + timedelta(0, 0, 1), TIMESTAMP_FORMAT
        )
        test_batch.data = [data]
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Inspector()
        sut.get_and_fill_data()
        sut.inspect()
        self.assertEqual([0, 0], sut.anomalies)
        self.assertTrue(isinstance(sut.model, RShashDetector))

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch(
        "src.inspector.inspector.MODELS",
        [
            {"model": "KNNDetector", "module": "streamad.model", "model_args": {}},
            {"model": "SpotDetector", "module": "streamad.model", "model_args": {}},
        ],
    )
    @patch(
        "src.inspector.inspector.ENSEMBLE",
        {
            "model": "WeightEnsemble",
            "module": "streamad.process",
            "model_args": {"ensemble_weights": [0.6, 0.4]},
        },
    )
    @patch("src.inspector.inspector.MODE", "ensemble")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_inspect_ensemble(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        test_batch = get_batch(None)
        test_batch.begin_timestamp = datetime.now()
        test_batch.end_timestamp = datetime.now() + timedelta(0, 0, 2)
        data = DEFAULT_DATA
        data["timestamp"] = datetime.strftime(
            test_batch.begin_timestamp + timedelta(0, 0, 1), TIMESTAMP_FORMAT
        )
        test_batch.data = [data]
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Inspector()
        sut.get_and_fill_data()
        sut.inspect()
        self.assertEqual([0, 0], sut.anomalies)

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch(
        "src.inspector.inspector.MODELS",
        [
            {
                "model": "KNNDetector",
                "module": "streamad.model",
                "model_args": {"window_len": 10},
            },
            {
                "model": "SpotDetector",
                "module": "streamad.model",
                "model_args": {"window_len": 10},
            },
        ],
    )
    @patch(
        "src.inspector.inspector.ENSEMBLE",
        {
            "model": "WeightEnsemble",
            "module": "streamad.process",
            "model_args": {"ensemble_weights": [0.6, 0.4]},
        },
    )
    @patch("src.inspector.inspector.MODE", "ensemble")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_inspect_ensemble_window_len(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        test_batch = get_batch(None)
        test_batch.begin_timestamp = datetime.now()
        test_batch.end_timestamp = datetime.now() + timedelta(0, 0, 2)
        data = DEFAULT_DATA
        data["timestamp"] = datetime.strftime(
            test_batch.begin_timestamp + timedelta(0, 0, 1), TIMESTAMP_FORMAT
        )
        test_batch.data = [data]
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Inspector()
        sut.get_and_fill_data()
        sut.inspect()
        self.assertNotEqual([None, None], sut.anomalies)

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch(
        "src.inspector.inspector.MODELS",
        [
            {"model": "RShashDetector", "module": "streamad.model", "model_args": {}},
            {"model": "SpotDetector", "module": "streamad.model", "model_args": {}},
        ],
    )
    @patch(
        "src.inspector.inspector.ENSEMBLE",
        {
            "model": "WeightEnsemble",
            "module": "streamad.process",
            "model_args": {"ensemble_weights": [0.6, 0.4]},
        },
    )
    @patch("src.inspector.inspector.MODE", "ensemble")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_inspect_ensemble_invalid(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        test_batch = get_batch(None)
        test_batch.begin_timestamp = datetime.now()
        test_batch.end_timestamp = datetime.now() + timedelta(0, 0, 2)
        data = DEFAULT_DATA
        data["timestamp"] = datetime.strftime(
            test_batch.begin_timestamp + timedelta(0, 0, 1), TIMESTAMP_FORMAT
        )
        test_batch.data = [data]
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Inspector()
        sut.get_and_fill_data()
        with self.assertRaises(NotImplementedError):
            sut.inspect()

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch(
        "src.inspector.inspector.MODELS",
        [{"model": "INVALID", "module": "streamad.model"}],
    )
    def test_invalid_model_univariate(
        self,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_clickhouse,
        mock_logger,
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Inspector()
        with self.assertRaises(NotImplementedError):
            sut.inspect()

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch(
        "src.inspector.inspector.MODELS",
        [{"model": "INVALID", "module": "streamad.model"}],
    )
    @patch("src.inspector.inspector.MODE", "multivariate")
    def test_invalid_model_multivariate(
        self,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_clickhouse,
        mock_logger,
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Inspector()
        with self.assertRaises(NotImplementedError):
            sut.inspect()

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch(
        "src.inspector.inspector.ENSEMBLE",
        {"model": "INVALID", "module": "streamad.process"},
    )
    @patch("src.inspector.inspector.MODE", "ensemble")
    def test_invalid_model_ensemble(
        self,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_clickhouse,
        mock_logger,
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Inspector()
        with self.assertRaises(NotImplementedError):
            sut.inspect()

    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.MODE", "INVALID")
    def test_invalid_mode(
        self, mock_kafka_consume_handler, mock_produce_handler, mock_clickhouse
    ):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut = Inspector()
        with self.assertRaises(NotImplementedError):
            sut.inspect()


class TestSend(unittest.TestCase):
    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.SCORE_THRESHOLD", 0.1)
    @patch("src.inspector.inspector.ANOMALY_THRESHOLD", 0.01)
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

        sut = Inspector()
        sut.anomalies = [0.9, 0.9]
        sut.X = np.array([[0.0], [0.0]])
        sut.begin_timestamp = datetime.now()
        sut.end_timestamp = datetime.now() + timedelta(0, 0, 2)
        data = DEFAULT_DATA
        data["timestamp"] = datetime.strftime(
            sut.begin_timestamp + timedelta(0, 0, 1), TIMESTAMP_FORMAT
        )
        sut.messages = [data]
        mock_batch_id = uuid.UUID("5ae0872e-5bb9-472c-8c37-8c173213a51f")
        with patch("src.inspector.inspector.uuid") as mock_uuid:
            mock_uuid.uuid4.return_value = mock_batch_id
            sut.send_data()

        mock_produce_handler_instance.produce.assert_called_once_with(
            topic="pipeline-inspector_to_detector",
            data=batch_schema.dumps(
                {
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
    @patch("src.inspector.inspector.SCORE_THRESHOLD", 0.1)
    @patch("src.inspector.inspector.ANOMALY_THRESHOLD", 0.01)
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

        sut = Inspector()
        sut.anomalies = [0.0, 0.0]
        sut.X = np.array([[0.0], [0.0]])
        sut.begin_timestamp = datetime.now()
        sut.end_timestamp = datetime.now() + timedelta(0, 0, 2)
        data = DEFAULT_DATA
        data["timestamp"] = datetime.strftime(
            sut.begin_timestamp + timedelta(0, 0, 1), TIMESTAMP_FORMAT
        )
        data["logline_id"] = uuid.UUID("99a427a6-ba3f-4aa2-b848-210d994d9108")
        sut.messages = [data]
        mock_batch_id = uuid.UUID("5ae0872e-5bb9-472c-8c37-8c173213a51f")
        with patch("src.inspector.inspector.uuid") as mock_uuid:
            mock_uuid.uuid4.return_value = mock_batch_id
            sut.send_data()

        mock_produce_handler_instance.produce.assert_not_called()


class TestMainFunction(unittest.TestCase):
    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.Inspector")
    def test_main_loop_execution(self, mock_inspector, mock_logger):
        # Arrange
        mock_inspector_instance = mock_inspector.return_value

        mock_inspector_instance.get_and_fill_data = MagicMock()
        mock_inspector_instance.clear_data = MagicMock()

        # Act
        main(one_iteration=True)

        # Assert
        self.assertTrue(mock_inspector_instance.get_and_fill_data.called)
        self.assertTrue(mock_inspector_instance.clear_data.called)

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.Inspector")
    def test_main_io_error_handling(self, mock_inspector, mock_logger):
        # Arrange
        mock_inspector_instance = mock_inspector.return_value

        # Act
        with patch.object(
            mock_inspector_instance,
            "get_and_fill_data",
            side_effect=IOError("Simulated IOError"),
        ):
            with self.assertRaises(IOError):
                main(one_iteration=True)

        # Assert
        self.assertTrue(mock_inspector_instance.clear_data.called)
        self.assertTrue(mock_inspector_instance.loop_exited)

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.Inspector")
    def test_main_value_error_handling(self, mock_inspector, mock_logger):
        # Arrange
        mock_inspector_instance = mock_inspector.return_value

        # Act
        with patch.object(
            mock_inspector_instance,
            "get_and_fill_data",
            side_effect=ValueError("Simulated ValueError"),
        ):
            main(one_iteration=True)

        # Assert
        self.assertTrue(mock_inspector_instance.clear_data.called)
        self.assertTrue(mock_inspector_instance.loop_exited)

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.Inspector")
    def test_main_keyboard_interrupt(self, mock_inspector, mock_logger):
        # Arrange
        mock_inspector_instance = mock_inspector.return_value
        mock_inspector_instance.get_and_fill_data.side_effect = KeyboardInterrupt

        # Act
        main()

        # Assert
        self.assertTrue(mock_inspector_instance.clear_data.called)
        self.assertTrue(mock_inspector_instance.loop_exited)


if __name__ == "__main__":
    unittest.main()
