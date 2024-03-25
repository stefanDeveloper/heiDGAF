import string
from dataclasses import dataclass, field
from typing import Callable, Tuple

import polars as pl
import torch
from torch.utils.data.dataset import Dataset


@dataclass
class Dataset:
    train_path: str
    val_path: str
    test_path: str
    cast_dataset: Callable
    binary: bool = field(default=True)

    @property
    def train(self):
        return {
            "train_path": self.train_path,
            "val_path": self.val_path,
            "cast_dataset": self.cast_dataset,
            "binary": self.binary,
        }

    @property
    def test(self):
        return {
            "test_path": self.test_path,
            "cast_dataset": self.cast_dataset,
            "binary": self.binary,
        }
