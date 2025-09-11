import math
import numpy as np
import unittest
from unittest.mock import MagicMock, patch, call

from src.detector.plugins.dga_detector import DGADetector
from src.base.data_classes.batch import Batch


DEFAULT_DATA = {
    "src_ip": "192.168.0.167",
    "dns_ip": "10.10.0.10",
    "response_ip": "252.79.173.222",
    "timestamp": "",
    "status": "NXDOMAIN",
    "domain_name": "IF356gEnJHPdRxnkDId4RDUSgtqxx9I+pZ5n1V53MdghOGQncZWAQgAPRx3kswi.750jnH6iSqmiAAeyDUMX0W6SHGpVsVsKSX8ZkKYDs0GFh/9qU5N9cwl00XSD8ID.NNhBdHZIb7nc0hDQXFPlABDLbRwkJS38LZ8RMX4yUmR2Mb6YqTTJBn+nUcB9P+v.jBQdwdS53XV9W2p1BHjh.16.f.1.6037.tunnel.example.org",
    "record_type": "A",
    "size": "100b",
}

class TestDGADetector(unittest.TestCase):
    def setUp(self):
        patcher = patch("src.detector.plugins.dga_detector.logger")
        self.mock_logger = patcher.start()
        self.addCleanup(patcher.stop)

    def _create_detector(self, mock_kafka_handler=None, mock_clickhouse=None):
        """Helper method to create a DGADetector instance with proper mocks."""
        if mock_kafka_handler is None:
            mock_kafka_handler = MagicMock()
        if mock_clickhouse is None:
            mock_clickhouse = MagicMock()
            
        detector_config = {
            "name": "dga_detector",
            "detector_module_name": "dga_detector",
            "detector_class_name": "DGADetector",
            "model": "rf",
            "checksum": "ba1f718179191348fe2abd51644d76191d42a5d967c6844feb3371b6f798bf06",
            "base_url": "https://heibox.uni-heidelberg.de/d/0d5cbcbe16cd46a58021",
            "threshold": 0.005
        }
        
        with patch('src.detector.detector.ExactlyOnceKafkaConsumeHandler', return_value=mock_kafka_handler), \
             patch('src.detector.detector.ClickHouseKafkaSender', return_value=mock_clickhouse), \
             patch.object(DGADetector, '_get_model', return_value=(MagicMock(),MagicMock())):
            
            detector = DGADetector(detector_config, "test_topic")
            detector.model = MagicMock()
            detector.scaler = MagicMock()
            return detector

    def test_get_model_download_url(self):
        """Test that the model download URL is correctly formatted."""
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        detector = self._create_detector(mock_kafka, mock_ch)
        # overwrite model here again to not interefere with other tests when using it globally
        detector.model = "rf"
        self.maxDiff = None
        expected_url = "https://heibox.uni-heidelberg.de/d/0d5cbcbe16cd46a58021/files/?p=%2Frf/ba1f718179191348fe2abd51644d76191d42a5d967c6844feb3371b6f798bf06/rf.pickle&dl=1"
        self.assertEqual(detector.get_model_download_url(), expected_url)


    def test_detect(self):
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        sut = self._create_detector(mock_kafka, mock_ch)
        sut.messages = [DEFAULT_DATA]
        with patch('src.detector.plugins.dga_detector.DGADetector.predict', return_value=[[0.01,0.99]]):
            sut.detect()
            self.assertNotEqual([], sut.warnings)


    def test_predict_calls_model(self):
        """Test that predict method correctly uses the model with features."""
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        detector = self._create_detector(mock_kafka, mock_ch)
        
        # Mock model prediction
        mock_prediction = np.array([[0.2, 0.8]])
        detector.model.predict_proba.return_value = mock_prediction
        
        # Test prediction
        message = {"domain_name": "google.com"}
        result = detector.predict(message)
        
        # Verify model was called once
        detector.model.predict_proba.assert_called_once()
        
        # Verify the argument was correct
        called_features = detector.model.predict_proba.call_args[0][0]
        expected_features = detector._get_features("google.com")
        np.testing.assert_array_equal(called_features, expected_features)
        
        # Verify prediction result
        np.testing.assert_array_equal(result, mock_prediction)


    def test_get_features_basic_attributes(self):
        """Test basic label features calculation."""
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        detector = self._create_detector(mock_kafka, mock_ch)
        
        # Test with 'google.com'
        features = detector._get_features("google.com")
        
        # Basic features: label_length, label_max, label_average
        label_length = features[0][0]  # 2 (google, com)
        label_max = features[0][1]     # 6 (google)
        label_average = features[0][2] # 10 (google.com)
        
        self.assertEqual(label_length, 2)
        self.assertEqual(label_max, 6)
        self.assertEqual(label_average, 10)  # 10 characters including dot

    def test_get_features_empty_domain(self):
        """Test handling of empty domain string."""
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        detector = self._create_detector(mock_kafka, mock_ch)
        
        features = detector._get_features("")
        
        # Basic features
        # Note: "".split(".") returns [""] so length is 1
        self.assertEqual(features[0][0], 1)  # label_length
        self.assertEqual(features[0][1], 0)  # label_max (empty string has length 0)
        self.assertEqual(features[0][2], 0)  # label_average (empty string)
        
        # Letter frequencies should all be 0
        for i in range(3, 29):  # letter frequency indices
            self.assertEqual(features[0][i], 0)

    def test_get_features_single_character(self):
        """Test handling of single character domain."""
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        detector = self._create_detector(mock_kafka, mock_ch)
        
        features = detector._get_features("a")
        
        # Basic features
        self.assertEqual(features[0][0], 1)  # label_length
        self.assertEqual(features[0][1], 1)  # label_max
        self.assertEqual(features[0][2], 1)  # label_average
        
        # Letter frequency for 'a' should be 1.0
        self.assertEqual(features[0][3], 1.0)  # 'a' is at index 3

    def test_get_features_feature_vector_shape(self):
        """Test that the feature vector has the expected shape."""
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        detector = self._create_detector(mock_kafka, mock_ch)
        
        features = detector._get_features("test.domain.com")
        
        expected_entropy = 44
        
        self.assertEqual(features.shape, (1, expected_entropy))

    def test_get_features_case_insensitivity(self):
        """Test that letter frequency calculation is case-insensitive."""
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        detector = self._create_detector(mock_kafka, mock_ch)
        
        features_upper = detector._get_features("GOOGLE.COM")
        features_lower = detector._get_features("google.com")
        
        # Letter frequencies should be identical regardless of case
        np.testing.assert_array_almost_equal(
            features_upper[0][3:29],  # letter frequency part
            features_lower[0][3:29],
            decimal=5
        )

    def test_get_features_subdomain_handling(self):
        """Test proper handling of different subdomains."""
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        detector = self._create_detector(mock_kafka, mock_ch)
        
        # Test with multiple subdomains
        features = detector._get_features("sub1.sub2.example.com")
        
        # Should have 4 labels (sub1, sub2, example, com)
        self.assertEqual(features[0][0], 4)  # label_length
        self.assertEqual(features[0][1], 7)  # label_max (example)
        self.assertEqual(features[0][2], 21)  # label_average (sub1.sub2.example.com has 21 characters)

    def test_entropy_calculation(self):
        """Test the entropy calculation function directly."""
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        detector = self._create_detector(mock_kafka, mock_ch)
        
        # Test with uniform distribution (max entropy)
        uniform = "abcd"
        uniform_entropy = -4 * (0.25 * math.log(0.25, 2))
        self.assertAlmostEqual(detector._get_features(uniform)[0][-3], uniform_entropy, delta=0.01)
        
        # Test with repetitive pattern (low entropy)
        repetitive = "aaaa"
        self.assertAlmostEqual(detector._get_features(repetitive)[0][-3], 0.0, delta=0.01)