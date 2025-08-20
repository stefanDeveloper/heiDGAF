import unittest
from unittest.mock import patch, MagicMock
import numpy as np
from src.inspector.plugins.no_inspector import NoInspector

class TestNoInspector(unittest.TestCase):
    @patch("src.inspector.inspector.logger")
    @patch("src.inspector.inspector.ExactlyOnceKafkaProduceHandler")
    @patch("src.inspector.inspector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.inspector.inspector.ClickHouseKafkaSender")    
    def setUp(self,
        mock_clickhouse,
        mock_kafka_consume_handler,
        mock_produce_handler,
        mock_logger,
    ):
        self.consume_topic = "test_topic"
        self.produce_topics = ["output_topic"]
        self.config = {
            "name": "test-no-inspector",
            "prefilter_name": "test-prefilter",
            "inspector_module_name": "no_inspector",
            "inspector_class_name": "NoInspector"
        }

        # Create a mock InspectorBase
        with patch('src.inspector.plugins.no_inspector.InspectorBase') as mock_base:
            # Set up the mock to have a messages attribute
            mock_base_instance = mock_base.return_value
            mock_base_instance.messages = []
            mock_base_instance.name = "NoInspector"
            
            # Create the NoInspector instance
            self.inspector = NoInspector(
                self.consume_topic,
                self.produce_topics,
                self.config
            )
            self.inspector.kafka_consume_handler = mock_kafka_consume_handler
            # Manually set up messages for testing
            self.inspector.messages = [
                {"domain_name": "example.com", "timestamp": "2025-01-01T00:00:00Z"},
                {"domain_name": "malicious-domain.xyz", "timestamp": "2025-01-01T00:00:01Z"}
            ]

    def test_init(self):
        # Verify constructor parameters were passed correctly
        self.assertEqual(self.inspector.consume_topic, self.consume_topic)
        self.assertEqual(self.inspector.produce_topics, self.produce_topics)
        self.assertEqual(self.inspector.name, self.config["name"])

    def test_inspect_anomalies(self):
        # Act
        self.inspector.inspect_anomalies()
        
        # Assert
        # Should set anomalies to an array of 1s with the same length as messages
        self.assertEqual(len(self.inspector.anomalies), len(self.inspector.messages))
        self.assertTrue(np.array_equal(self.inspector.anomalies, np.array([1, 1])))
        self.assertTrue(all(anomaly == 1 for anomaly in self.inspector.anomalies))

    def test_inspect(self):
        # Mock inspect_anomalies to verify it's called
        with patch.object(self.inspector, 'inspect_anomalies') as mock_inspect_anomalies:
            # Act
            self.inspector.inspect()
            
            # Assert
            mock_inspect_anomalies.assert_called_once()

    def test_get_models(self):
        # Act & Assert
        # Should not raise any exceptions
        try:
            self.inspector._get_models(["model1", "model2"])
        except Exception as e:
            self.fail(f"_get_models() raised {type(e).__name__} unexpectedly!")

    @patch('src.inspector.plugins.no_inspector.logger')
    def test_subnet_is_suspicious(self, mock_logger):
        # Arrange
        self.inspector.anomalies = np.array([1, 1, 1])
        
        # Act
        result = self.inspector.subnet_is_suspicious()
        
        # Assert
        mock_logger.info.assert_called_once_with(f"{self.inspector.name}: 3 anomalies found")
        self.assertTrue(result)
        
    def test_subnet_is_suspicious_with_empty_messages(self):
        # Arrange
        self.inspector.messages = []
        self.inspector.anomalies = np.array([])
        
        # Act
        result = self.inspector.subnet_is_suspicious()
        
        # Assert
        self.assertTrue(result)
        
    def test_inspect_flow(self):
        # Act
        self.inspector.inspect()
        
        # Assert
        # After inspect, anomalies should be set
        self.assertEqual(len(self.inspector.anomalies), len(self.inspector.messages))
        self.assertTrue(np.array_equal(self.inspector.anomalies, np.array([1, 1])))
        
        # And subnet_is_suspicious should work
        self.assertTrue(self.inspector.subnet_is_suspicious())