import os

import pandas as pd

from torch.utils.data.dataset import Dataset

class MajesticMillionDataset(Dataset):
    def __init__(self, csv_file) -> None:
        self.data = pd.read_csv(csv_file)
    
    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx) -> any:
        position = self.data.iloc[idx, 0]
        print(position)
        return position
