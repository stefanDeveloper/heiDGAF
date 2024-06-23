import logging

from heidgaf_core.kafka_handler import KafkaConsumeHandler, KafkaMessageFetchException
from heidgaf_core.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


class Inspector:
    def __init__(self):
        self.messages = []
        self.busy = False
        self.kafka_consume_handler = KafkaConsumeHandler(topic='Inspect')

    def get_and_fill_data(self):
        if self.busy:
            logger.warning("Inspector is busy! Not consuming new messages.")
            return

        self.messages = self.kafka_consume_handler.consume_and_return_json_data()

        if self.messages:
            self.busy = True

    def clear_data(self):
        self.messages = []
        self.busy = False

        logger.info("Cleared messages. Inspector is now available.")


# TODO: Test
def main():
    inspector = Inspector()

    while True:
        try:
            logger.debug("Before getting and filling messages")
            inspector.get_and_fill_data()
            logger.debug("After getting and filling messages")
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
