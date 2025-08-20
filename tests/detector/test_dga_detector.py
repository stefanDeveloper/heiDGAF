import math
import numpy as np
import unittest
from unittest.mock import MagicMock, patch, call

from src.detector.plugins.dga_detector import DGADetector
from src.base.data_classes.batch import Batch


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
             patch.object(DGADetector, '_get_model', return_value=MagicMock()):
            
            detector = DGADetector(detector_config, "test_topic")
            detector.model = MagicMock()
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

    def test_get_features_letter_frequency(self):
        """Test letter frequency distribution calculation."""
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        detector = self._create_detector(mock_kafka, mock_ch)
        
        # Test with 'abcabc'
        features = detector._get_features("abcabc")
        
        # Letter frequency should show a, b, c each at ~0.333
        alphabet = "abcdefghijklmnopqrstuvwxyz"
        a_idx = alphabet.index('a')
        b_idx = alphabet.index('b')
        c_idx = alphabet.index('c')
        
        self.assertAlmostEqual(features[0][3], 2/6, delta=0.01)  # a
        self.assertAlmostEqual(features[0][4], 2/6, delta=0.01)  # b
        self.assertAlmostEqual(features[0][5], 2/6, delta=0.01)  # c

    def test_get_features_character_types(self):
        """Test calculation of character type counts."""
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        detector = self._create_detector(mock_kafka, mock_ch)
        
        # Test with 'test123!.com'
        features = detector._get_features("test123!.com")
        
        # We need to calculate the indices for the character type features
        # After basic features (3) and letter frequencies (26), we have frequency stats (4)
        # Then character type features start
        
        # For FQDN level (the whole domain)
        full_count_idx = 3 + 26 + 4  # basic + letter freq + freq stats
        alpha_count_idx = full_count_idx + 1
        numeric_count_idx = full_count_idx + 2
        special_count_idx = full_count_idx + 3
        
        # FQDN is 'test123!.com' (12 characters)
        self.assertEqual(features[0][full_count_idx], 12)
        # Alpha: 'testcom' = 7 characters -> 7/12 ≈ 0.583
        self.assertAlmostEqual(features[0][alpha_count_idx], 7/12, delta=0.01)
        # Numeric: '123' = 3 characters -> 3/12 = 0.25
        self.assertAlmostEqual(features[0][numeric_count_idx], 3/12, delta=0.01)
        # Special: '!. ' = 2 characters -> 2/12 ≈ 0.167
        self.assertAlmostEqual(features[0][special_count_idx], 2/12, delta=0.01)

    def test_get_features_entropy_calculation(self):
        """Test entropy calculation for different domains."""
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        detector = self._create_detector(mock_kafka, mock_ch)
        
        # High entropy domain (more random-looking)
        high_entropy = detector._get_features("xkcd1234.randomdomain.biz")
        # Low entropy domain (repetitive pattern)
        low_entropy = detector._get_features("aaaaa.aaaaa.com")
        
        # Entropy features should be at the end of the feature vector
        # We need to calculate the index position
        # After basic (3) + letter freq (26) + freq stats (4) + 
        # char type counts (3 levels * 4 = 12) + stats (3 levels * 4 = 12)
        entropy_idx = 3 + 26 + 4 + 12 + 12
        
        # High entropy domain should have higher entropy value
        self.assertGreater(high_entropy[0][entropy_idx], low_entropy[0][entropy_idx])

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
        
        # Calculate expected feature count:
        # basic: 3
        # letter frequencies: 26
        # frequency stats: 4
        # character type counts (3 levels * 4): 12
        # stats for counts (3 levels * 4): 12
        # entropy (3 levels): 3
        expected_length = 3 + 26 + 4 + 12 + 12 + 3
        
        self.assertEqual(features.shape, (1, expected_length))

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