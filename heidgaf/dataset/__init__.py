import logging
from dataclasses import dataclass
from typing import Any, Callable, List

import polars as pl
import sklearn.model_selection
from torch.utils.data.dataset import Dataset


def cast_cic(data_path: List[str]):
    dataframes = []
    for data in data_path:
        y = data.split("_")[-1].split(".")[0]
        df = pl.read_csv(data, has_header=False)
        df = df.with_columns(
            [
                pl.lit(y).alias("class")
            ]
        )
        df = df.rename(
            {
                "column_1": "query"
            }
        )
        dataframes.append(df)
    return pl.concat(dataframes)

def cast_dgta(data_path: str) -> pl.DataFrame:
    def __custom_decode(data):
        retL=[None] * len(data)
        for i, datum in enumerate(data):
            retL[i]=str(datum.decode('latin-1').encode('utf-8').decode('utf-8'))

        return(pl.Series(retL))
    
    df = pl.read_parquet(data_path)
    df = df.rename({"domain": "query"})

    # Drop unnecessary column
    df = df.drop("__index_level_0__")
    df = df.with_columns(
        [
            pl.col('query').map(__custom_decode)
        ]
    )
    return df

@dataclass
class Dataset:
    def __init__(self, data_path: Any, cast_dataset: Callable = None) -> None:
        if cast_dataset != None:
            self.data = cast_dataset(data_path)
        else:
            self.data = pl.read_csv(data_path)
            
        self.X_train, self.X_val, self.X_test, self.Y_train, self.Y_val, self.Y_test = self.__train_test_val_split()

    def __len__(self):
        return len(self.data)
    
    def __train_test_val_split(self, train_frac=0.8, random_state=None):
        X_train, X_tmp, Y_train, Y_tmp = sklearn.model_selection.train_test_split(
            self.data.drop("class"), 
            self.data.select("class"), 
            train_size=train_frac, 
            random_state=random_state
        )

        X_val, X_test, Y_val, Y_test = sklearn.model_selection.train_test_split(
            X_tmp,
            Y_tmp,
            train_size=0.5,
            random_state=random_state
        )

        return X_train, X_val, X_test, Y_train, Y_val, Y_test

    @property
    def train(self):
        return {
            "X": self.X_train,
            "Y": self.Y_train
        }

    @property
    def test(self):
        return {
            "X": self.X_test,
            "Y": self.Y_test
        }

    @property
    def val(self):
        return {
            "X": self.X_val,
            "Y": self.Y_val
        }

dgta_dataset = Dataset(
    data_path="/home/smachmeier/projects/heiDGA/data/dgta/dgta-benchmark.parquet",
    cast_dataset=cast_dgta
)

cic_dataset = Dataset(
    data_path=[
        "/home/smachmeier/projects/heiDGA/example/CICBellDNS2021_CSV_benign.csv",
        "/home/smachmeier/projects/heiDGA/example/CICBellDNS2021_CSV_malware.csv",
        "/home/smachmeier/projects/heiDGA/example/CICBellDNS2021_CSV_phishing.csv",
        "/home/smachmeier/projects/heiDGA/example/CICBellDNS2021_CSV_spam.csv"
    ],
    cast_dataset=cast_cic
)

