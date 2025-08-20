import unittest
from unittest.mock import patch, MagicMock, mock_open, call
from src.inspector.plugins.stream_ad_inspector import StreamADInspector
import importlib
import os
import sys
import uuid
from datetime import datetime, timedelta
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
from streamad.model import ZScoreDetector, RShashDetector

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


def create_test_batch():
    test_batch = Batch(
                batch_tree_row_id=f"{uuid.uuid4()}-{uuid.uuid4()}",
                batch_id=uuid.uuid4(),
                begin_timestamp=datetime.now(),
                end_timestamp=datetime.now(),
                data=[],
            )
    test_batch.begin_timestamp = datetime.now()
    test_batch.end_timestamp = datetime.now() + timedelta(0, 0, 2)
    data = DEFAULT_DATA
    data["ts"] = datetime.isoformat(
        test_batch.begin_timestamp + timedelta(0, 0, 1)
    )
    return test_batch

class TestStreamAdInspectorSetup(unittest.TestCase):
    @patch("src.inspector.inspector.PLUGIN_PATH", "src.inspector.plugins")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.logger")
    def setUp(self, mock_logger,mock_consume_handler, mock_produce_handler, mock_clickhouse):
        """Initialize inspector instance before each test."""
        self.inspector = StreamADInspector(
            consume_topic="consume_topic",
            produce_topics=["produce_topic"],
            config={
                "inspector_class_name": "StreamADInspector",
                "inspector_module_name": "stream_ad_inspector",
                "mode": "univariate",
                "anomaly_threshold": 0.05,
                "score_threshold": 0.05,
                "time_type": "ms",
                "time_range": 20,
                "name": "test-inspector",
                "ensemble": {
                    "model": "WeightEnsemble",
                    "module": "streamad.process",
                    "model_args": ""            
                }
            }
        )
    def test_init_attributes(self):
        """Verify initial state of core attribute."""
        self.assertTrue(self.inspector.ensemble_config["model"] == "WeightEnsemble")
        self.assertTrue(self.inspector.name == "test-inspector")
        self.assertTrue(self.inspector.mode == "univariate")


class TestStreamAdInspectorInvalidSetup(unittest.TestCase):
    @patch("src.inspector.inspector.PLUGIN_PATH", "src.inspector.plugins")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.logger")
    def setUp(self, mock_logger,mock_consume_handler, mock_produce_handler, mock_clickhouse):
            self.consume_topic="consume_topic",
            self.produce_topics=["produce_topic"],
            self.config={
                "inspector_class_name": "StreamADInspector",
                "inspector_module_name": "stream_ad_inspector",
                "mode": "univariate",
                "anomaly_threshold": 0.05,
                "score_threshold": 0.05,
                "time_type": "ms",
                "time_range": 20,
                "name": "test-inspector",
                "ensemble": {
                    "model": "WeightEnsemble",
                    "module": "streamad.process",
                    "model_args": ""            
                }
            }
        
    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_inspect_ensemble_invalid(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        self.config["models"] = [
            {"model": "RShashDetector", "module": "streamad.model", "model_args": {}},
            {"model": "SpotDetector", "module": "streamad.model", "model_args": {}},
        ]
        self.config["mode"] = "ensemble"
        self.config["ensemble"] = {
            "model": "WeightEnsemble",
            "module": "streamad.process",
            "model_args": {"ensemble_weights": [0.6, 0.4]},
        },
        sut = StreamADInspector(
            self.consume_topic, self.produce_topics, self.config
        )

        test_batch = create_test_batch()
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        sut.kafka_consume_handler = mock_kafka_consume_handler_instance
        
        mock_produce_handler_instance = MagicMock()
        mock_produce_handler.return_value = mock_produce_handler_instance

        sut.get_and_fill_data()
        with self.assertRaises(NotImplementedError):
            sut.inspect()


    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    def test_invalid_model_univariate(
        self,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_clickhouse,
        mock_logger,
    ):
        self.config["models"] = [{"model": "INVALID", "module": "streamad.model"}]
        # mock_kafka_consume_handler_instance = MagicMock()
        # mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        # mock_produce_handler_instance = MagicMock()
        # mock_produce_handler.return_value = mock_produce_handler_instance
        sut = StreamADInspector(
            self.consume_topic, self.produce_topics, self.config
        )
        with self.assertRaises(NotImplementedError):
            sut.inspect()
            
            
    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    def test_invalid_model_multivariate(
        self,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_clickhouse,
        mock_logger,
    ):
        self.config["models"] = [{"model": "INVALID", "module": "streamad.model"}]
        self.config["mode"] = "multivariate"
        sut = StreamADInspector(
            self.consume_topic, self.produce_topics, self.config
        )

        with self.assertRaises(NotImplementedError):
            sut.inspect()
            
   
    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")   
    def test_invalid_model_ensemble(
        self,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_clickhouse,
        mock_logger,
    ):
        # mock_kafka_consume_handler_instance = MagicMock()
        # mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        # mock_produce_handler_instance = MagicMock()
        # mock_produce_handler.return_value = mock_produce_handler_instance
        self.config["ensemble"] = [{"model": "INVALID", "module": "streamad.process"}]
        self.config["mode"] = "ensemble"

        sut = StreamADInspector(
            self.consume_topic, self.produce_topics, self.config
        )

        with self.assertRaises(NotImplementedError):
            sut.inspect()


class TestStreamAdInspectorMeanPacketSize(unittest.TestCase):
    @patch("src.inspector.inspector.PLUGIN_PATH", "src.inspector.plugins")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.logger")
    def setUp(self, mock_logger,mock_consume_handler, mock_produce_handler, mock_clickhouse):
        """Initialize inspector instance before each test."""
        self.sut = StreamADInspector(
            consume_topic="consume_topic",
            produce_topics=["produce_topic"],
            config={
                "inspector_class_name": "StreamADInspector",
                "inspector_module_name": "stream_ad_inspector",
                "mode": "univariate",
                "anomaly_threshold": 0.05,
                "score_threshold": 0.05,
                "time_type": "ms",
                "time_range": 20,
                "name": "test-inspector",
                "ensemble": {
                    "model": "WeightEnsemble",
                    "module": "streamad.process",
                    "model_args": ""            
                }
            }
        )
    def test_count_errors(self):
        begin_timestamp = datetime.now()
        end_timestamp = datetime.now() + timedelta(0, 0, 2)
        data = DEFAULT_DATA
        data["ts"] = datetime.isoformat(begin_timestamp + timedelta(0, 0, 1))
        messages = [data]
        np.testing.assert_array_equal(
            np.asarray([[1.0], [0.0]]),
            self.sut._count_errors(messages, begin_timestamp, end_timestamp),
        )
    def test_mean_packet_size(
        self):
        begin_timestamp = datetime.now()
        end_timestamp = datetime.now() + timedelta(0, 0, 2)
        data = DEFAULT_DATA
        data["ts"] = datetime.isoformat(begin_timestamp + timedelta(0, 0, 1))
        messages = [data]
        np.testing.assert_array_equal(
            np.asarray([[100], [0.0]]),
            self.sut._mean_packet_size(messages, begin_timestamp, end_timestamp),
        )
        
    def test_count_errors_empty_messages(
        self):

        begin_timestamp = datetime.now()
        end_timestamp = datetime.now() + timedelta(0, 0, 2)
        data = DEFAULT_DATA
        data["ts"] = datetime.isoformat(begin_timestamp + timedelta(0, 0, 1))
        np.testing.assert_array_equal(
            np.asarray([[0.0], [0.0]]),
            self.sut._count_errors([], begin_timestamp, end_timestamp),
        )

    def test_mean_packet_size_empty_messages(self):
        begin_timestamp = datetime.now()
        end_timestamp = begin_timestamp + timedelta(0, 0, 2)
        data = DEFAULT_DATA
        data["ts"] = datetime.isoformat(begin_timestamp + timedelta(0, 0, 1))
        np.testing.assert_array_equal(
            np.asarray([[0.0], [0.0]]),
            self.sut._mean_packet_size([], begin_timestamp, end_timestamp),
        )
        
class TestInspectFunction(unittest.TestCase):
    def setUp(self):
            self.consume_topic="consume_topic",
            self.produce_topics=["produce_topic"],
            self.config={
                "inspector_class_name": "StreamADInspector",
                "inspector_module_name": "stream_ad_inspector",
                "mode": "univariate",
                "anomaly_threshold": 0.05,
                "score_threshold": 0.05,
                "time_type": "ms",
                "time_range": 20,
                "name": "test-inspector",
                "ensemble": {
                    "model": "WeightEnsemble",
                    "module": "streamad.process",
                    "model_args": ""            
                }
            }
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.logger")
    def test_inspect_none_models(self, mock_logger,mock_consume_handler, mock_produce_handler, mock_clickhouse):
        self.config["models"] = None

        sut = StreamADInspector(
            self.consume_topic, self.produce_topics, self.config
        )
        test_batch = create_test_batch()
        
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        sut.kafka_consume_handler = mock_kafka_consume_handler_instance

        sut.get_and_fill_data()
        with self.assertRaises(NotImplementedError):
            sut.inspect()

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    def test_inspect_empty_models(
        self,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_clickhouse,
        mock_logger,
    ):
        self.config["models"] = []

        sut = StreamADInspector(
            self.consume_topic, self.produce_topics, self.config
        )
        test_batch = create_test_batch()
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        sut.kafka_consume_handler = mock_kafka_consume_handler_instance

        sut.get_and_fill_data()

        with self.assertRaises(NotImplementedError):
            sut.inspect()

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_inspect_univariate(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        self.config["models"] = [{"model": "ZScoreDetector", "module": "streamad.model", "model_args": {}}]
        self.config["mode"] = "univariate"
        sut = StreamADInspector(
            self.consume_topic, self.produce_topics, self.config
        )
        test_batch = create_test_batch()
        
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        sut.kafka_consume_handler = mock_kafka_consume_handler_instance

        sut.get_and_fill_data()
        sut.inspect()
        self.assertEqual([0, 0], sut.anomalies)

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_inspect_univariate_model_init(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        self.config["models"] = [{"model": "ZScoreDetector", "module": "streamad.model", "model_args": {}}]
        self.config["mode"] = "univariate"
        sut = StreamADInspector(
            self.consume_topic, self.produce_topics, self.config
        )
        test_batch = create_test_batch()
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        sut.kafka_consume_handler = mock_kafka_consume_handler_instance
        
        sut.get_and_fill_data()
        sut.models=sut._get_models(
            [{"model": "ZScoreDetector", "module": "streamad.model", "model_args": {}}]
        )
        self.assertEqual(ZScoreDetector, type(sut.models[0]))

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_inspect_univariate_2(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        self.config["models"] = [{"model": "ZScoreDetector","module": "streamad.model","model_args": {"window_len": 10}}]
        self.config["mode"] = "univariate"        
        sut = StreamADInspector(
            self.consume_topic, self.produce_topics, self.config
        )
        test_batch = create_test_batch()
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        sut.kafka_consume_handler = mock_kafka_consume_handler_instance

        sut.get_and_fill_data()
        sut.inspect()
        self.assertNotEqual([None, None], sut.anomalies)

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_inspect_univariate_two_models(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        self.config["models"] = [{"model": "ZScoreDetector", "module": "streamad.model", "model_args": {}}, {"model": "KNNDetector", "module": "streamad.model", "model_args": {}}]
        self.config["mode"] = "univariate"        
        sut = StreamADInspector(
            self.consume_topic, self.produce_topics, self.config
        )
        test_batch = create_test_batch()
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        sut.kafka_consume_handler = mock_kafka_consume_handler_instance


        sut.get_and_fill_data()
        sut.inspect()
        self.assertEqual([0, 0], sut.anomalies)
        self.assertTrue(isinstance(sut.models[0], ZScoreDetector))

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_inspect_multivariate(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        self.config["models"] = [{"model": "RShashDetector", "module": "streamad.model", "model_args": {}}]
        self.config["mode"] = "multivariate"        
        sut = StreamADInspector(
            self.consume_topic, self.produce_topics, self.config
        )
        test_batch = create_test_batch()
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        sut.kafka_consume_handler = mock_kafka_consume_handler_instance


        sut.get_and_fill_data()
        sut.inspect()
        self.assertEqual([0, 0], sut.anomalies)

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_inspect_multivariate_window_len(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        self.config["models"] = [{"model": "RShashDetector","module": "streamad.model","model_args": {"window_len": 10}}]
        self.config["mode"] = "multivariate"        
        sut = StreamADInspector(
            self.consume_topic, self.produce_topics, self.config
        )
        test_batch = create_test_batch()
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        sut.kafka_consume_handler = mock_kafka_consume_handler_instance

        sut.get_and_fill_data()
        sut.inspect()
        self.assertNotEqual([None, None], sut.anomalies)

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_inspect_multivariate_two_models(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        self.config["models"] = [{"model": "RShashDetector", "module": "streamad.model", "model_args": {}},{"model": "xStreamDetector", "module": "streamad.model", "model_args": {}}]
        self.config["mode"] = "multivariate"        
        sut = StreamADInspector(
            self.consume_topic, self.produce_topics, self.config
        )
        test_batch = create_test_batch()
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        sut.kafka_consume_handler = mock_kafka_consume_handler_instance


        sut.get_and_fill_data()
        sut.inspect()
        self.assertEqual([0, 0], sut.anomalies)
        self.assertTrue(isinstance(sut.models[0], RShashDetector))

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_inspect_ensemble(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        self.config["models"] = [{"model": "KNNDetector", "module": "streamad.model", "model_args": {}},{"model": "SpotDetector", "module": "streamad.model", "model_args": {}}]
        self.config["ensemble"] = {"model": "WeightEnsemble","module": "streamad.process","model_args": {"ensemble_weights": [0.6, 0.4]}}
        self.config["mode"] = "ensemble"        
        sut = StreamADInspector(
            self.consume_topic, self.produce_topics, self.config
        )
        test_batch = create_test_batch()
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        sut.kafka_consume_handler = mock_kafka_consume_handler_instance

        sut.get_and_fill_data()
        sut.inspect()
        self.assertEqual([0, 0], sut.anomalies)

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_inspect_ensemble_model_init(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        self.config["models"] = [{"model": "KNNDetector","module": "streamad.model","model_args": {"window_len": 10},},{"model": "SpotDetector","module": "streamad.model","model_args": {"window_len": 10},}]
        self.config["ensemble"] = {"model": "WeightEnsemble","module": "streamad.process","model_args": {"ensemble_weights": [0.6, 0.4]}}
        self.config["mode"] = "ensemble"        
        sut = StreamADInspector(
            self.consume_topic, self.produce_topics, self.config
        )
        test_batch = create_test_batch()
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        sut.kafka_consume_handler = mock_kafka_consume_handler_instance

        sut.get_and_fill_data()
        sut._get_ensemble()
        ensemble = sut.ensemble
        sut.ensemble = None
        sut._get_ensemble()
        self.assertEqual(type(ensemble), type(sut.ensemble))
        ensemble = sut.ensemble
        sut._get_ensemble()
        self.assertEqual(ensemble, sut.ensemble)

    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")
    def test_inspect_ensemble_window_len(
        self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        self.config["models"] = [{"model": "KNNDetector","module": "streamad.model","model_args": {"window_len": 10},},{"model": "SpotDetector","module": "streamad.model","model_args": {"window_len": 10},}]
        self.config["ensemble"] = {"model": "WeightEnsemble","module": "streamad.process","model_args": {"ensemble_weights": [0.6, 0.4]}}
        self.config["mode"] = "ensemble"        
        sut = StreamADInspector(
            self.consume_topic, self.produce_topics, self.config
        )
        test_batch = create_test_batch()
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )
        sut.kafka_consume_handler = mock_kafka_consume_handler_instance


        sut.get_and_fill_data()
        sut.inspect()
        self.assertNotEqual([None, None], sut.anomalies)


