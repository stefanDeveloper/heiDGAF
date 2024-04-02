import os
import string

import polars as pl
from torch.utils.data.dataset import Dataset

from heidgaf.cache import DataFrameRedisCache


class MajesticMillionDataset(Dataset):
    def __init__(self, csv_file: str = "/home/smachmeier/projects/heiDGA/data/majestic_million/majestic_million.csv") -> None:
        self.data = pl.read_csv(csv_file)
    
    def __len__(self) -> int:
        return len(self.data)

    def __getitem__(self, idx: int) -> pl.DataFrame:
        return self.data[idx, 0]
    
    def __call__(self, name: str, key: str) -> pl.DataFrame:
        return self.data.filter(pl.col(key) == name)