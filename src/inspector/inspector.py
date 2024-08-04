import logging
import os
import sys

sys.path.append(os.getcwd())
from src.base.kafka_handler import KafkaConsumeHandler, KafkaMessageFetchException
from src.base.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


class Inspector:
    def __init__(self, subnet_id: str) -> None:
        self.begin_timestamp = None
        self.end_timestamp = None
        self.messages = []

        logger.debug(f"Initializing Inspector...")
        consume_topic_name = "Inspect_" + subnet_id
        logger.debug(f"Calling KafkaConsumeHandler(topic='{consume_topic_name}')...")
        self.kafka_consume_handler = KafkaConsumeHandler(topic=consume_topic_name)
        logger.debug(f"Initialized Inspector.")

    def get_and_fill_data(self) -> None:
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
            self.begin_timestamp = data.get("begin_timestamp")
            self.end_timestamp = data.get("end_timestamp")
            self.messages = data.get("data")

        if not self.messages:
            logger.info("Received message:\n"
                        "    ⤷  Empty data field: No unfiltered data available.")
        else:
            logger.info("Received message:\n"
                        f"    ⤷  Contains data field of {len(self.messages)} message(s).")

        logger.debug("Received consumer message as json data.")
        logger.debug(f"(data={self.messages})")

    def clear_data(self):
        self.messages = []
        self.begin_timestamp = None
        self.end_timestamp = None
        logger.debug("Cleared messages and timestamps. Inspector is now available.")


# TODO: Test
def main():
    logger.info("Starting Inspector...")
    subnet_id = "192.168.0.0_24"  # TODO: Change! Only for testing!
    inspector = Inspector(subnet_id=subnet_id)
    logger.info(f"Inspector is running.\n    ⤷  Inspecting the subnet with subnet_id {subnet_id}.")

    while True:
        try:
            logger.debug("Before getting and filling data")
            inspector.get_and_fill_data()
            logger.debug("After getting and filling data")

            logger.debug("Functionality not yet implemented.")
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
