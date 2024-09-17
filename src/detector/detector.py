import ast
from datetime import datetime
from enum import Enum, unique
import hashlib
import json
import logging
import os
import sys
import importlib
import joblib
import numpy as np

import requests
from streamad.util import StreamGenerator, CustomDS

sys.path.append(os.getcwd())
from src.base.utils import setup_config
from src.base.kafka_handler import (
    KafkaConsumeHandler,
    KafkaMessageFetchException,
    KafkaProduceHandler,
)
from src.base.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

BUF_SIZE = 65536  # lets read stuff in 64kb chunks!

config = setup_config()
MODEL = config["heidgaf"]["detector"]["model"]
MODEL_PATH = config["heidgaf"]["detector"]["path"]


class Detector:
    def __init__(self) -> None:
        self.topic = "detector"

    def get_and_fill_data(self) -> None:
        """Consumes data from KafkaConsumeHandler and stores it for processing."""
        logger.debug("Getting and filling data...")
        if self.messages:
            logger.warning(
                "Inspector is busy: Not consuming new messages. Wait for the Inspector to finish the "
                "current workload."
            )
            return

        logger.debug(
            "Inspector is not busy: Calling KafkaConsumeHandler to consume new JSON messages..."
        )
        key, data = self.kafka_consume_handler.consume_and_return_json_data()

        if data:
            # TODO Fix convertion of data
            self.begin_timestamp = data.get("begin_timestamp")
            self.end_timestamp = data.get("end_timestamp")
            self.messages = data.get("data")
            self.key = key

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

        logger.debug("Received consumer message as json data.")
        logger.debug(f"(data={self.messages})")

    def _sha256sum(self, file_path):
        h = hashlib.sha256()

        with open(file_path, "rb") as file:
            while True:
                # Reading is buffered, so we can read smaller chunks.
                chunk = file.read(h.block_size)
                if not chunk:
                    break
                h.update(chunk)

        return h.hexdigest()

    def _get_model() -> None:
        """Downloads model from server. If model already exists, it returns the current model. In addition, it checks the sha256 sum in case a model has been updated."""

        if not os.path.isfile(f"/tmp/{MODEL}.pkl"):
            response = requests.get(f"{MODEL_PATH}/files/?p=%2F{MODEL}.pkl&dl=1")

            response.raise_for_status()

            # Check file sha256

        with open(rf"/tmp/{MODEL}.pkl", "wb") as f:
            f.write(response.content)

        return joblib.load(f"/tmp/{MODEL}.pkl")

    def detect(self) -> None:
        pass


def main(one_iteration: bool = False):
    """
    Creates the :class:`Detector` instance. Starts a loop that continously fetches data.

    Args:
        one_iteration (bool): For testing purposes: stops loop after one iteration

    Raises:
        KeyboardInterrupt: Execution interrupted by user. Closes down the :class:`LogCollector` instance.
    """
    logger.info("Starting Inspector...")
    detector = Detector()
    logger.info(f"Inspector is running.")

    iterations = 0

    while True:
        if one_iteration and iterations > 0:
            break
        iterations += 1

        try:
            logger.debug("Before getting and filling data")
            detector.get_and_fill_data()

        except KafkaMessageFetchException as e:  # pragma: no cover
            logger.debug(e)
        except IOError as e:
            logger.error(e)
            raise e
        except ValueError as e:
            logger.debug(e)
        except KeyboardInterrupt:
            logger.info("Closing down Inspector...")
            break
        finally:
            detector.clear_data()


if __name__ == "__main__":  # pragma: no cover
    main()
