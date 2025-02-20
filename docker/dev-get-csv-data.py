import gzip
import os
import sys

sys.path.append(os.getcwd())
from src.train.dataset import DatasetLoader
from src.base.log_config import get_logger

logger = get_logger()


def create_dgta_dataset_json_gz(base_path="../data"):
    logger.info("Loading data for DGTA dataset...")
    try:
        loader = DatasetLoader(base_path)
        dataset = loader.dgta_dataset

        logger.info("Converting to JSON data...")
        json_data = dataset.data.write_json()
        logger.warning(json_data)

        logger.info("Compressing data...")
        with gzip.open("../data/dgta_dataset.json.gz", "wb") as f:
            logger.info("Writing compressed data to file...")
            f.write(json_data.encode())
    except FileNotFoundError:
        logger.warning(
            "Dataset was not found in 'data' directory. Skipping this dataset"
        )
        return
    except Exception as err:
        logger.error(err)
        return

    logger.info("DGTA dataset: Done")


if __name__ == "__main__":
    create_dgta_dataset_json_gz()
