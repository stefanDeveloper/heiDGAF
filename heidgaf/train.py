import logging
from enum import Enum

import numpy as np
import torch
from fe_polars.encoding.target_encoding import TargetEncoder
from fe_polars.imputing.base_imputing import Imputer
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from torch.utils.data import DataLoader

from heidgaf.cache import DataFrameRedisCache
from heidgaf.dataset.dgta import DGTA
from heidgaf.dataset.majestic import MajesticMillionDataset
from heidgaf.models import Pipeline
from heidgaf.models.lr import LogisticRegression
from heidgaf.post.feature import Preprocessor


class ModelType(Enum):
    LOGISTIC_REGRESSION = LogisticRegression

class DNSAnalyzerTraining:
    def __init__(self, model: ModelType, redis_host="localhost", redis_port=6379, redis_db=0, redis_max_connections=20) -> None:
        self.redis_cache = DataFrameRedisCache(redis_host, redis_port, redis_db, redis_max_connections)
        
    def train(self, seed=42):
        
        if seed > 0:
            np.random.seed(seed)
            torch.manual_seed(seed)

        # setting device on GPU if available, else CPU
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

        logging.info(f'Using device: {device}')
        if torch.cuda.is_available():
            logging.info("GPU detected")
            logging.info(f"\t{torch.cuda.get_device_name(0)}")
            
        if device.type == 'cuda':
            logging.info("Memory Usage:")
            logging.info(f"\tAllocated: {round(torch.cuda.memory_allocated(0)/1024**3,1)} GB")
            logging.info(f"\tCached:    {round(torch.cuda.memory_reserved(0)/1024**3,1)} GB")
        
        # TODO Load data set
        logging.info(f'Loading data sets')
        majestic_dataset = MajesticMillionDataset(self.redis_cache)
        dgta_dataset = DGTA()
        
        # TODO Handle Data loader
        train_dataloader = DataLoader(majestic_dataset, batch_size=64, shuffle=True)
        test_dataloader = DataLoader(majestic_dataset, batch_size=64, shuffle=True)
        # train_features, train_labels = next(iter(train_dataloader))
        
        # Training model
        model_pipeline = Pipeline(
            preprocessor=Preprocessor(
                features_to_drop=[]),
            mean_imputer=Imputer(
                features_to_impute=["", ""], strategy="mean"),
            target_encoder=TargetEncoder(
                smoothing=100,
                features_to_encode=["", "", "", "", "",""]),
            clf=LogisticRegression(input_dim=9, output_dim=1, epochs=5000)
        )
        
        # train, target, test = data_loader()
        # x_train, x_val, y_train, y_val = train_test_split(train, target, test_size=0.33, random_state=seed)
        
        # model_pipeline.fit(x_train=x_train, y_train=y_train)
        

    