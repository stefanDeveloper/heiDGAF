import logging
import os
import sys

sys.path.append(os.getcwd())  # needed for Terminal execution
from heidgaf_core.kafka_handler import KafkaConsumeHandler, KafkaMessageFetchException
from heidgaf_core.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


class Inspector:
    def __init__(self):
        logger.debug(f"Initializing Inspector...")
        self.messages = []
        logger.debug(f"Calling KafkaConsumeHandler(topic='Inspect')...")
        self.kafka_consume_handler = KafkaConsumeHandler(topic="Inspect")
        logger.debug(f"Initialized Inspector.")

    def get_and_fill_data(self):
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
        self.messages = self.kafka_consume_handler.consume_and_return_json_data()

        if not self.messages:
            logger.debug("Received empty data from KafkaConsumeHandler.")

        logger.debug("Received data from KafkaConsumeHandler.")
        logger.debug(f"(data={self.messages})")

    def clear_data(self):
        self.messages = []
        logger.info("Cleared messages. Inspector is now available.")


# TODO: Test
def main():
    inspector = Inspector()

    while True:
        try:
            inspector.get_and_fill_data()
            # TODO: Implement functionality here
        except IOError as e:
            logger.error(e)
            raise
        except ValueError as e:
            logger.debug(e)
        except KafkaMessageFetchException as e:
            logger.debug(e)
            continue
        except KeyboardInterrupt:
            logger.info("Closing down Inspector...")
            break
        finally:
            inspector.clear_data()


if __name__ == "__main__":
    main()
