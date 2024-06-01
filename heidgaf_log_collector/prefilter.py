import json

from heidgaf_log_collector.batch_handler import KafkaBatchSender


class InspectPrefilter:
    def __init__(self, error_type: str):
        self.unfiltered_data = []
        self.filtered_data = []
        self.error_type = error_type

        self.batch_handler = KafkaBatchSender(topic="Inspect")
        self.batch_handler.start_kafka_producer()

    def consume_and_extract_data(self):
        pass

    def filter_by_error(self):
        for e in self.unfiltered_data:
            if e["status"] == self.error_type:
                self.filtered_data.append(e)

    def add_filtered_data_to_batch(self):
        if not self.filtered_data:
            raise ValueError("Failed to add data to batch: No filtered data.")

        self.batch_handler.add_message(json.dumps(self.filtered_data.copy()))

    def clear_data(self):
        self.unfiltered_data = []
        self.filtered_data = []
