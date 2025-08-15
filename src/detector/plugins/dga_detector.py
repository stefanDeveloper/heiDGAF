from src.detector.detector import DetectorBase
import math
import numpy as np

from src.base.log_config import get_logger
module_name = "data_analysis.detector"
logger = get_logger(module_name)

class DGADetector(DetectorBase):    

    def __init__(self,detector_config, consume_topic): 
        self.model_base_url = detector_config["base_url"]
        super().__init__(detector_config, consume_topic)

    def get_model_download_url(self):
        return f"{self.model_base_url}/files/?p=%2F{self.model}_{self.checksum}.pickle&dl=1"

    def predict(self, message):
        y_pred = self.model.predict_proba(
                self._get_features(message["domain_name"])
        )
        return y_pred


    def _get_features(self, query: str):
        """Transform a dataset with new features using numpy.

        Args:
            query (str): A string to process.

        Returns:
            dict: Preprocessed data with computed features.
        """

        # Splitting by dots to calculate label length and max length
        label_parts = query.split(".")
        label_length = len(label_parts)
        label_max = max(len(part) for part in label_parts)
        label_average = len(query.strip("."))

        logger.debug("Get letter frequency")
        alc = "abcdefghijklmnopqrstuvwxyz"
        freq = np.array(
            [query.lower().count(i) / len(query) if len(query) > 0 else 0 for i in alc]
        )

        logger.debug("Get full, alpha, special, and numeric count.")

        def calculate_counts(level: str) -> np.ndarray:
            if len(level) == 0:
                return np.array([0, 0, 0, 0])

            full_count = len(level)
            alpha_count = sum(c.isalpha() for c in level) / full_count
            numeric_count = sum(c.isdigit() for c in level) / full_count
            special_count = (
                sum(not c.isalnum() and not c.isspace() for c in level) / full_count
            )

            return np.array([full_count, alpha_count, numeric_count, special_count])

        levels = {
            "fqdn": query,
            "thirdleveldomain": label_parts[0] if len(label_parts) > 2 else "",
            "secondleveldomain": label_parts[1] if len(label_parts) > 1 else "",
        }
        counts = {
            level: calculate_counts(level_value)
            for level, level_value in levels.items()
        }

        logger.debug("Get frequency standard deviation, median, variance, and mean.")
        freq_std = np.std(freq)
        freq_var = np.var(freq)
        freq_median = np.median(freq)
        freq_mean = np.mean(freq)

        logger.debug(
            "Get standard deviation, median, variance, and mean for full, alpha, special, and numeric count."
        )
        stats = {}
        for level, count_array in counts.items():
            stats[f"{level}_std"] = np.std(count_array)
            stats[f"{level}_var"] = np.var(count_array)
            stats[f"{level}_median"] = np.median(count_array)
            stats[f"{level}_mean"] = np.mean(count_array)

        logger.debug("Start entropy calculation")

        def calculate_entropy(s: str) -> float:
            if len(s) == 0:
                return 0
            probabilities = [float(s.count(c)) / len(s) for c in dict.fromkeys(list(s))]
            entropy = -sum(p * math.log(p, 2) for p in probabilities)
            return entropy

        entropy = {level: calculate_entropy(value) for level, value in levels.items()}

        logger.debug("Finished entropy calculation")

        # Final feature aggregation as a NumPy array
        basic_features = np.array([label_length, label_max, label_average])
        freq_features = np.array([freq_std, freq_var, freq_median, freq_mean])

        # Flatten counts and stats for each level into arrays
        level_features = np.hstack([counts[level] for level in levels.keys()])
        stats_features = np.array(
            [stats[f"{level}_std"] for level in levels.keys()]
            + [stats[f"{level}_var"] for level in levels.keys()]
            + [stats[f"{level}_median"] for level in levels.keys()]
            + [stats[f"{level}_mean"] for level in levels.keys()]
        )

        # Entropy features
        entropy_features = np.array([entropy[level] for level in levels.keys()])

        # Concatenate all features into a single numpy array
        all_features = np.concatenate(
            [
                basic_features,
                freq,
                freq_features,
                level_features,
                stats_features,
                entropy_features,
            ]
        )

        logger.debug("Finished data transformation")

        return all_features.reshape(1, -1)