import os

import polars as pl
from torch.utils.data.dataset import Dataset

from heidgaf.cache import DataFrameRedisCache


class MajesticMillionDataset(Dataset):
    def __init__(self, csv_file, redis_cache: DataFrameRedisCache) -> None:
        self.data = pl.read_csv(csv_file)
        self.redis_cache = redis_cache
    
    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx: int) -> any:
        return self.data[idx, 0]
