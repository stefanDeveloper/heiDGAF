import os

import polars as pl

from torch.utils.data.dataset import Dataset

class MajesticMillionDataset(Dataset):
    def __init__(self, csv_file) -> None:
        self.data = pl.read_csv(csv_file)
    
    def cache():
        pass
    
    
    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx) -> any:
        position = self.data.iloc[idx, 0]
        print(position)
        return position
