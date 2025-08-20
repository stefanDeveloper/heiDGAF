from src.inspector.inspector import InspectorBase
import numpy as np
from src.base.log_config import get_logger

module_name = "data_inspection.inspector"
logger = get_logger(module_name)
# TODO: test this!
class NoInspector(InspectorBase):    
    def __init__(self, consume_topic, produce_topics, config) -> None:
        super().__init__(consume_topic, produce_topics, config)
        
    def inspect_anomalies(self) -> None:
        # declare everything to be suspicious
        self.anomalies = np.array([1 for message in self.messages])
    # TODO: send data needs to be either revised or values be set for it !
    def inspect(self) -> None:
        self.inspect_anomalies()
    def _get_models(self, models):
        pass
    def subnet_is_suspicious(self) -> bool: 
        logger.info(f"{self.name}: {len(self.anomalies)} anomalies found")
        return True
