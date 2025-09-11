import os
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, mock_open

from requests import HTTPError

from src.base.data_classes.batch import Batch
from src.detector.detector import DetectorBase, WrongChecksum
from src.base.kafka_handler import KafkaMessageFetchException

MINIMAL_DETECTOR_CONFIG={
    "name": "test-detector",
      "detector_module_name": "test_detector",
      "detector_class_name": "TestDetector",
      "model": "rf",
      "checksum": "ba1f718179191348fe2abd51644d76191d42a5d967c6844feb3371b6f798bf06",
      "base_url": "https://heibox.uni-heidelberg.de/d/0d5cbcbe16cd46a58021/",
      "threshold": "0.005",
      "inspector_name": "no_inspector",
}


class TestDetector(DetectorBase):
    """
        Testclass that does not take any action to not dialute the tests
    """
    def __init__(self, detector_config, consume_topic) -> None:
        self.model_base_url = detector_config["base_url"]
        super().__init__(consume_topic=consume_topic,detector_config=detector_config)

    def get_model_download_url(self):
        return f"{self.model_base_url}/files/?p=%2F{self.model}_{self.checksum}.pickle&dl=1"
    def get_scaler_download_url(self):
        return f"{self.model_base_url}/files/?p=%2F{self.model}_{self.checksum}_scaler.pickle&dl=1"
          
    def predict(self, message):
        pass    

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


class TestSha256Sum(unittest.TestCase):
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.detector.detector.ClickHouseKafkaSender")
    @patch("src.detector.detector.DetectorBase._get_model")
    def test_sha256_empty_file(self, mock_get_model, mock_clickhouse, mock_kafka_consume_handler):
        mock_get_model.return_value = (MagicMock(),MagicMock())
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance


        sut = TestDetector(
            consume_topic="test_topic",
            detector_config=MINIMAL_DETECTOR_CONFIG
        )
        with self.assertRaises(FileNotFoundError):
            sut._sha256sum("")

    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.detector.detector.ClickHouseKafkaSender")
    @patch("src.detector.detector.DetectorBase._get_model")
    def test_sha256_not_existing_file(
        self, mock_get_model, mock_clickhouse, mock_kafka_consume_handler
    ):
        mock_get_model.return_value=(MagicMock(),MagicMock())
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance


        sut = TestDetector(
            consume_topic="test_topic",
            detector_config=MINIMAL_DETECTOR_CONFIG
        )
        with self.assertRaises(FileNotFoundError):
            sut._sha256sum("not_existing")

class TestGetModel(unittest.TestCase):
    def setUp(self):
        patcher = patch("src.detector.detector.logger")
        self.mock_logger = patcher.start()
        self.addCleanup(patcher.stop)

    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.detector.detector.ClickHouseKafkaSender")
    def test_get_model(self, mock_clickhouse, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance


        sut = TestDetector(
            consume_topic="test_topic",
            detector_config=MINIMAL_DETECTOR_CONFIG
        )

    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.detector.detector.ClickHouseKafkaSender")
    def test_get_model_wrong_checksum(self, mock_clickhouse, mock_kafka_consume_handler):
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        detector_config = MINIMAL_DETECTOR_CONFIG.copy()
        detector_config["checksum"] ="INVALID"
        with self.assertRaises(WrongChecksum):
            sut = TestDetector(
            consume_topic="test_topic",
            detector_config=detector_config
        )
class TestInit(unittest.TestCase):
    # @patch("src.detector.detector.CONSUME_TOPIC", "test_topic")
    @patch("src.detector.detector.logger")
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.detector.detector.ClickHouseKafkaSender")
    @patch("src.detector.detector.DetectorBase._get_model")
    def test_init(self, mock_get_model, mock_clickhouse, mock_kafka_consume_handler, mock_logger):
        mock_get_model.return_value=(MagicMock(),MagicMock())
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = TestDetector(
            consume_topic="test_topic",
            detector_config=MINIMAL_DETECTOR_CONFIG
        )

        self.assertEqual([], sut.messages)
        self.assertEqual(mock_kafka_consume_handler_instance, sut.kafka_consume_handler)
        mock_kafka_consume_handler.assert_called_once_with("test_topic")


class TestGetData(unittest.TestCase):
    @patch("src.detector.detector.logger")
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.detector.detector.ClickHouseKafkaSender")
    @patch("src.detector.detector.DetectorBase._get_model")
    def test_get_data_without_return_data(
        self, mock_get_model,mock_clickhouse, mock_kafka_consume_handler, mock_logger
    ):
        mock_get_model.return_value=(MagicMock(),MagicMock())
        test_batch = Batch(
            batch_tree_row_id=f"{uuid.uuid4()}-{uuid.uuid4()}",
            batch_id=uuid.uuid4(),
            begin_timestamp=datetime.now(),
            end_timestamp=datetime.now() + timedelta(0, 3),
            data=[],
        )

        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "test",
            test_batch,
        )

        sut = TestDetector(
            consume_topic="test_topic",
            detector_config=MINIMAL_DETECTOR_CONFIG
        )
        sut.parent_row_id = f"{uuid.uuid4()}-{uuid.uuid4()}"
        sut.get_and_fill_data()

        self.assertEqual([], sut.messages)

    @patch("src.detector.detector.logger")
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.detector.detector.ClickHouseKafkaSender")
    @patch("src.detector.detector.DetectorBase._get_model")
    def test_get_data_with_return_data(
        self, mock_get_model, mock_clickhouse, mock_kafka_consume_handler, mock_logger
    ):
        mock_get_model.return_value=(MagicMock(),MagicMock())
        begin = datetime.now()
        end = begin + timedelta(0, 3)
        test_batch = Batch(
            batch_tree_row_id=f"{uuid.uuid4()}-{uuid.uuid4()}",
            batch_id=uuid.uuid4(),
            begin_timestamp=begin,
            end_timestamp=end,
            data=[{"test": "test_message_2"}],
        )

        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_as_object.return_value = (
            "192.168.1.0/24",
            test_batch,
        )

        sut = TestDetector(
            consume_topic="test_topic",
            detector_config=MINIMAL_DETECTOR_CONFIG
        )
        sut.messages = []
        sut.get_and_fill_data()

        self.assertEqual(begin, sut.begin_timestamp)
        self.assertEqual(end, sut.end_timestamp)
        self.assertEqual([{"test": "test_message_2"}], sut.messages)

    @patch("src.detector.detector.logger")
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.detector.detector.ClickHouseKafkaSender")
    @patch("src.detector.detector.DetectorBase._get_model")
    def test_get_data_while_busy(
        self, mock_get_model, mock_clickhouse, mock_kafka_consume_handler, mock_logger
    ):
        mock_get_model.return_value=(MagicMock(),MagicMock())
        begin = datetime.now()
        end = begin + timedelta(0, 3)
        test_batch = Batch(
            batch_tree_row_id=f"{uuid.uuid4()}-{uuid.uuid4()}",
            batch_id=uuid.uuid4(),
            begin_timestamp=begin,
            end_timestamp=end,
            data=[{"test": "test_message_2"}],
        )

        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_and_return_object.return_value = (
            "192.168.1.0/24",
            test_batch,
        )

        sut = TestDetector(
            consume_topic="test_topic",
            detector_config=MINIMAL_DETECTOR_CONFIG
        )
        sut.messages = [{"test": "test_message_2"}]
        sut.get_and_fill_data()

        self.assertEqual([{"test": "test_message_2"}], sut.messages)


class TestSendWarning(unittest.TestCase):
    def setUp(self):
        patcher = patch("src.detector.detector.logger")
        self.mock_logger = patcher.start()
        self.addCleanup(patcher.stop)

    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.detector.detector.ClickHouseKafkaSender")
    @patch("src.detector.detector.DetectorBase._get_model")
    def test_save_warning(self, mock_get_model, mock_clickhouse, mock_kafka_consume_handler):
        mock_get_model.return_value=(MagicMock(),MagicMock())
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = TestDetector(consume_topic="test_topic", detector_config=MINIMAL_DETECTOR_CONFIG
        )
        sut.warnings = [
            {
                "request": "google.de",
                "probability": 0.8765,
                "model": "rf",
                "sha256": "021af76b2385ddbc76f6e3ad10feb0bb081f9cf05cff2e52333e31040bbf36cc",
            },
            {
                "request": "request.de",
                "probability": 0.12388,
                "model": "rf",
                "sha256": "021af76b2385ddbc76f6e3ad10feb0bb081f9cf05cff2e52333e31040bbf36cc",
            },
        ]
        sut.parent_row_id = f"{uuid.uuid4()}-{uuid.uuid4()}"
        sut.messages = [{"logline_id": "test_id"}]
        open_mock = mock_open()
        with patch("src.detector.detector.open", open_mock, create=True):
            sut.send_warning()

        open_mock.assert_called_with(
            os.path.join(tempfile.gettempdir(), "warnings.json"), "a+"
        )

    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.detector.detector.ClickHouseKafkaSender")
    @patch("src.detector.detector.DetectorBase._get_model")
    def test_save_empty_warning(self, mock_get_model, mock_clickhouse, mock_kafka_consume_handler):
        mock_get_model.return_value=(MagicMock(),MagicMock())
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = TestDetector(
            consume_topic="test_topic",
            detector_config=MINIMAL_DETECTOR_CONFIG
        )
        sut.parent_row_id = f"{uuid.uuid4()}-{uuid.uuid4()}"
        sut.warnings = []
        sut.messages = [{"logline_id": "test_id"}]
        open_mock = mock_open()
        with patch("src.detector.detector.open", open_mock, create=True):
            sut.send_warning()

        open_mock.assert_not_called()

    # @patch(
    #     "src.detector.detector.CHECKSUM",
    #     "ba1f718179191348fe2abd51644d76191d42a5d967c6844feb3371b6f798bf06",
    # )
    # @patch("src.detector.detector.MODEL", "rf")
    # @patch(
    #     "src.detector.detector.MODEL_BASE_URL",
    #     "https://heibox.uni-heidelberg.de/d/0d5cbcbe16cd46a58021/",
    # )
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.detector.detector.ClickHouseKafkaSender")
    @patch("src.detector.detector.DetectorBase._get_model")
    def test_save_warning_error(self, mock_get_model, mock_clickhouse, mock_kafka_consume_handler):
        mock_get_model.return_value=(MagicMock(),MagicMock())
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance

        sut = TestDetector(consume_topic="test_topic",detector_config=MINIMAL_DETECTOR_CONFIG)
        sut.warnings = [
            {
                "request": "request.de",
                "probability": "INVALID",
                "model": "rf",
                "sha256": "021af76b2385ddbc76f6e3ad10feb0bb081f9cf05cff2e52333e31040bbf36cc",
            }
        ]
        with self.assertRaises(Exception):
            sut.send_warning()


class TestClearData(unittest.TestCase):
    def setUp(self):
        patcher = patch("src.detector.detector.logger")
        self.mock_logger = patcher.start()
        self.addCleanup(patcher.stop)

    @patch("src.detector.detector.logger")
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.detector.detector.ClickHouseKafkaSender")
    @patch("src.detector.detector.DetectorBase._get_model")
    def test_clear_data_without_existing_data(
        self, mock_get_model, mock_clickhouse, mock_kafka_consume_handler, mock_logger
    ):
        mock_get_model.return_value=(MagicMock(),MagicMock())
        begin = datetime.now()
        end = begin + timedelta(0, 3)
        test_batch = Batch(
            batch_tree_row_id=f"{uuid.uuid4()}-{uuid.uuid4()}",
            batch_id=uuid.uuid4(), begin_timestamp=begin, end_timestamp=end, data=[]
        )

        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_and_return_object.return_value = (
            "192.168.1.0/24",
            test_batch,
        )

        sut = TestDetector(
            consume_topic="test_topic",
            detector_config=MINIMAL_DETECTOR_CONFIG
        )
        
        sut.messages = []
        sut.clear_data()

        self.assertEqual([], sut.messages)

    @patch("src.detector.detector.logger")
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.detector.detector.ClickHouseKafkaSender")
    @patch("src.detector.detector.DetectorBase._get_model")
    def test_clear_data_with_existing_data(
        self, mock_get_model,mock_clickhouse, mock_kafka_consume_handler, mock_logger
    ):
        mock_get_model.return_value=(MagicMock(),MagicMock())
        begin = datetime.now()
        end = begin + timedelta(0, 3)
        test_batch = Batch(
            batch_tree_row_id=f"{uuid.uuid4()}-{uuid.uuid4()}",
            batch_id=uuid.uuid4(), begin_timestamp=begin, end_timestamp=end, data=[]
        )

        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        mock_kafka_consume_handler_instance.consume_and_return_object.return_value = (
            "192.168.1.0/24",
            test_batch,
        )

        sut = TestDetector(
            consume_topic="test_topic",
            detector_config=MINIMAL_DETECTOR_CONFIG
        )
        sut.messages = ["test_data"]
        sut.begin_timestamp = datetime.now()
        sut.end_timestamp = sut.begin_timestamp + timedelta(0, 3)
        sut.clear_data()

        self.assertEqual([], sut.messages)
        self.assertIsNone(sut.begin_timestamp)
        self.assertIsNone(sut.end_timestamp)



class TestGetModelMethod(unittest.TestCase):
    def setUp(self):
        patcher = patch("src.detector.detector.logger")
        self.mock_logger = patcher.start()
        self.addCleanup(patcher.stop)

    @patch("src.detector.detector.requests.get")
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.detector.detector.ClickHouseKafkaSender")
    def test_get_model_downloads_and_validates(
        self, mock_clickhouse, mock_kafka_consume_handler, mock_requests_get
    ):
        """Test that model is downloaded when not present and checksum is validated."""
        # Setup mock response with valid model content
        mock_response = MagicMock()
        mock_response.content = b"mock model content"
        mock_requests_get.return_value = mock_response
        
        # Create test detector instance
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        
        sut = TestDetector(
            consume_topic="test_topic",
            detector_config=MINIMAL_DETECTOR_CONFIG
        )
        
        # Mock file operations
        with patch("src.detector.detector.os.path.isfile", return_value=False), \
             patch("src.detector.detector.open", mock_open()), \
             patch("src.detector.detector.pickle.load", return_value="mock_model_or_scaler"), \
             patch.object(sut, "_sha256sum", return_value=MINIMAL_DETECTOR_CONFIG["checksum"]):
            
            model = sut._get_model()
            
            # Verify download was attempted
            mock_requests_get.assert_called()
            # Verify model was loaded
            self.assertEqual(model, ("mock_model_or_scaler","mock_model_or_scaler"))
            # Verify logger messages
            self.mock_logger.info.assert_any_call(f"Get model: {sut.model} with checksum {sut.checksum}")
            self.mock_logger.info.assert_any_call(f"downloading model {sut.model} from {sut.get_model_download_url()} with checksum {sut.checksum}")

    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.detector.detector.ClickHouseKafkaSender")
    def test_get_model_uses_existing_file(self, mock_clickhouse, mock_kafka_consume_handler):
        """Test that existing model file is used without re-downloading."""
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        
        sut = TestDetector(
            consume_topic="test_topic",
            detector_config=MINIMAL_DETECTOR_CONFIG
        )
        
        # Mock file operations
        with patch("src.detector.detector.os.path.isfile", return_value=True), \
             patch("src.detector.detector.open", mock_open()), \
             patch("src.detector.detector.pickle.load", return_value="mock_model_or_scaler"), \
             patch.object(sut, "_sha256sum", return_value=MINIMAL_DETECTOR_CONFIG["checksum"]), \
             patch("src.detector.detector.requests.get") as mock_requests_get:
            
            model_and_scaler = sut._get_model()
            
            # Verify no download was attempted
            mock_requests_get.assert_not_called()
            # Verify model was loaded
            self.assertEqual(model_and_scaler, ("mock_model_or_scaler", "mock_model_or_scaler"))
            # Verify logger messages
            self.mock_logger.info.assert_any_call(f"Get model: {sut.model} with checksum {sut.checksum}")

    @patch("src.detector.detector.requests.get")
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.detector.detector.ClickHouseKafkaSender")
    def test_get_model_raises_wrong_checksum(
        self, mock_clickhouse, mock_kafka_consume_handler, mock_requests_get
    ):
        """Test that WrongChecksum exception is raised when checksums don't match."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.content = b"mock model content"
        mock_requests_get.return_value = mock_response
        
        # Create test detector instance
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        
        sut = TestDetector(
            consume_topic="test_topic",
            detector_config=MINIMAL_DETECTOR_CONFIG
        )
        
        # Mock file operations with wrong checksum
        with patch("src.detector.detector.os.path.isfile", return_value=False), \
             patch("src.detector.detector.open", mock_open()), \
             patch.object(sut, "_sha256sum", return_value="wrong_checksum_value"):
            
            with self.assertRaises(WrongChecksum) as context:
                sut._get_model()
            
            # Verify exception message
            self.assertIn("Checksum", str(context.exception))
            self.assertIn("is not equal with new checksum", str(context.exception))
            # Verify logger warning
            self.mock_logger.warning.assert_called_once()

    @patch("src.detector.detector.requests.get")
    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.detector.detector.ClickHouseKafkaSender")
    def test_get_model_handles_http_error(
        self, mock_clickhouse, mock_kafka_consume_handler, mock_requests_get
    ):
        """Test that HTTP errors during download are properly propagated."""
        # Setup mock to raise HTTP error
        mock_requests_get.side_effect = HTTPError("Download failed")
        
        # Create test detector instance
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        
        sut = TestDetector(
            consume_topic="test_topic",
            detector_config=MINIMAL_DETECTOR_CONFIG
        )
        
        # Mock file operations
        with patch("src.detector.detector.os.path.isfile", return_value=False), \
             patch("src.detector.detector.open", mock_open()):
            
            with self.assertRaises(HTTPError):
                sut._get_model()
            
            # Verify logger info was called
            self.mock_logger.info.assert_any_call(f"Get model: {sut.model} with checksum {sut.checksum}")

class TestBootstrapDetectorInstance(unittest.TestCase):
    def setUp(self):
        patcher = patch("src.detector.detector.logger")
        self.mock_logger = patcher.start()
        self.addCleanup(patcher.stop)

    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.detector.detector.ClickHouseKafkaSender")
    @patch("src.detector.detector.DetectorBase._get_model")
    def test_bootstrap_normal_execution(
        self, mock_get_model, mock_clickhouse, mock_kafka_consume_handler
    ):
        """Test normal execution flow of bootstrap_detector_instance."""
        mock_get_model.return_value = (MagicMock(),MagicMock())
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        
        sut = TestDetector(
            consume_topic="test_topic",
            detector_config=MINIMAL_DETECTOR_CONFIG
        )
        sut.kafka_consume_handler = mock_kafka_consume_handler_instance
        
        # Mock the methods called in the loop
        with patch.object(sut, "get_and_fill_data") as mock_get_data, \
             patch.object(sut, "detect") as mock_detect, \
             patch.object(sut, "send_warning", side_effect=KeyboardInterrupt) as mock_send_warning, \
             patch.object(sut, "clear_data") as mock_clear_data:
            
            try:
                sut.bootstrap_detector_instance()
            except KeyboardInterrupt:
                pass
            
            # Verify method call sequence
            mock_get_data.assert_called_once()
            mock_detect.assert_called_once()
            mock_send_warning.assert_called_once()
            mock_clear_data.assert_called_once()

    @patch("src.detector.detector.ExactlyOnceKafkaConsumeHandler")
    @patch("src.detector.detector.ClickHouseKafkaSender")
    @patch("src.detector.detector.DetectorBase._get_model")
    def test_bootstrap_graceful_shutdown(
        self, mock_get_model, mock_clickhouse, mock_kafka_consume_handler
    ):
        """Test graceful shutdown on KeyboardInterrupt in bootstrap_detector_instance."""
        mock_get_model.return_value = (MagicMock(),MagicMock())
        mock_kafka_consume_handler_instance = MagicMock()
        mock_kafka_consume_handler.return_value = mock_kafka_consume_handler_instance
        
        sut = TestDetector(
            consume_topic="test_topic",
            detector_config=MINIMAL_DETECTOR_CONFIG
        )
        
        # Mock methods and raise KeyboardInterrupt
        with patch.object(sut, "get_and_fill_data"), \
             patch.object(sut, "detect"), \
             patch.object(sut, "send_warning", side_effect=KeyboardInterrupt), \
             patch.object(sut, "clear_data") as mock_clear_data:
            
            # Should not raise exception as it's handled internally
            sut.bootstrap_detector_instance()
            
            # Verify shutdown message
            self.mock_logger.info.assert_called_with("Closing down Detector...")
            # Verify clear_data was called
            mock_clear_data.assert_called_once()