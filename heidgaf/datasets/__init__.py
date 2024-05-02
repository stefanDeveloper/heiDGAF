import logging
from dataclasses import dataclass
from typing import Any, Callable, List

import numpy as np
import polars as pl
import sklearn.model_selection
from fe_polars.encoding.one_hot_encoding import OneHotEncoder
from torch.utils.data.dataset import Dataset


def preprocess(x: pl.DataFrame):
    x = x.with_columns(
        [
            (pl.col("query").str.split(".").alias("labels")),
        ]
    )
    
    x = x.with_columns(
        [
            (pl.col("labels").list.get(-1).alias("tld")),
        ]
    )

    x = x.with_columns(
        [
            # FQDN
            (pl.col("query")).alias("fqdn"),
        ]
    )

    x = x.with_columns(
        [
            # Second-level domain
            (
                pl.when(pl.col("labels").list.len() > 2)
                .then(pl.col("labels").list.get(-2))
                .otherwise(pl.col("labels").list.get(0))
                .alias("secondleveldomain")
            )
        ]
    )

    x = x.with_columns(
        [
            # Third-level domain
            (
                pl.when(pl.col("labels").list.len() > 2)
                .then(
                    pl.col("labels")
                    .list.slice(0, pl.col("labels").list.len() - 2)
                    .list.join(".")
                )
                .otherwise(pl.lit(""))
                .alias("thirdleveldomain")
            ),
        ]
    )
    x = x.with_columns(
        [
            (
                pl.when(pl.col("class") == "legit")
                .then(pl.lit(0))
                .otherwise(pl.lit(1))
                .alias("class")
            )
        ]
    )
 
    return x


def cast_dga(data_path: str):
    df = pl.read_csv(data_path)
    df = df.rename({"Domain": "query"})
    df = df.drop(["DGA_family", "Type"])
    df = df.with_columns([pl.lit("malicious").alias("class")])
    df = preprocess(df)
    return df

def cast_bambenek(data_path: str):
    df = pl.read_csv(data_path)
    df = df.rename({"Domain": "query"})
    df = df.drop(["DGA_family", "Type"])
    df = df.with_columns([pl.lit("malicious").alias("class")])
    df = preprocess(df)
    return df

def cast_cic(data_path: List[str]):
    dataframes = []
    for data in data_path:
        y = data.split("_")[-1].split(".")[0]
        df = pl.read_csv(data, has_header=False)
        if y == "benign":
            df = df.with_columns([pl.lit("legit").alias("class")])
        else:
            df = df.with_columns([pl.lit(y).alias("class")])
        df = df.rename({"column_1": "query"})
        df = preprocess(df)
        dataframes.append(df)
    return pl.concat(dataframes)


def cast_dgta(data_path: str) -> pl.DataFrame:
    def __custom_decode(data):
        retL = [None] * len(data)
        for i, datum in enumerate(data):
            retL[i] = str(datum.decode("latin-1").encode("utf-8").decode("utf-8"))

        return pl.Series(retL)

    df = pl.read_parquet(data_path)
    df = df.rename({"domain": "query"})

    # Drop unnecessary column
    df = df.drop("__index_level_0__")
    df = df.with_columns([pl.col("query").map(__custom_decode)])
    df = preprocess(df)
    return df


@dataclass
class Dataset:
    """Dataset class."""

    def __init__(
        self, data_path: Any, data: pl.DataFrame = None, cast_dataset: Callable = None
    ) -> None:
        """Initializes data.

        Either a valid data_path is given to load data or the provided data is set. If callback for preprocessing is set, the callback is run by cast_dataset(data_path).


        Args:
            data_path (Any): _description_
            data (pl.DataFrame, optional): _description_. Defaults to None.
            cast_dataset (Callable, optional): _description_. Defaults to None.

        Raises:
            NotImplementedError: _description_
        """
        if cast_dataset != None:
            self.data = cast_dataset(data_path)
        elif data_path != "":
            self.data = pl.read_csv(data_path)
        elif not data is None:
            self.data = data
        else:
            raise NotImplementedError("No data given")
        self.label_encoder = OneHotEncoder(features_to_encode=["class"])
        self.X_train, self.X_val, self.X_test, self.Y_train, self.Y_val, self.Y_test = (
            self.__train_test_val_split()
        )

    def __len__(self) -> int:
        """Returns the length of data set.

        Returns:
            int: Length of the data set
        """
        return len(self.data)

    def __train_test_val_split(self, train_frac: float = 0.8, random_state: int = None) -> tuple[list, list, list, list, list, list]:
        """Splits data set in train, test, and validation set

        Args:
            train_frac (float, optional): Training fraction. Defaults to 0.8.
            random_state (int, optional): Random state. Defaults to None.

        Returns:
            tuple[list, list, list, list, list, list]: X_train, X_val, X_test, Y_train, Y_val, Y_test
        """

        self.data = self.data.filter(pl.col("query").str.len_chars() > 0)
        self.data = self.data.unique(subset="query")
        
        X_train, X_tmp, Y_train, Y_tmp = sklearn.model_selection.train_test_split(
            self.data.drop("class"),
            self.data.select("class"),
            train_size=train_frac,
            random_state=random_state,
        )

        X_val, X_test, Y_val, Y_test = sklearn.model_selection.train_test_split(
            X_tmp, Y_tmp, train_size=0.5, random_state=random_state
        )

        return X_train, X_val, X_test, Y_train, Y_val, Y_test

    @property
    def train(self) -> dict:
        """Training set

        Returns:
            dict: dictionary with features and labels.
        """
        return {"X": self.X_train, "Y": self.Y_train}

    @property
    def test(self) -> dict:
        return {"X": self.X_test, "Y": self.Y_test}

    @property
    def val(self) -> dict:
        return {"X": self.X_val, "Y": self.Y_val}


dgta_dataset = Dataset(
    data_path="/home/smachmeier/projects/heiDGA/data/dgta/dgta-benchmark.parquet",
    cast_dataset=cast_dgta,
)

dga_dataset = Dataset(
    data_path="/home/smachmeier/projects/heiDGA/data/360_dga_domain.csv",
    cast_dataset=cast_dga,
)

bambenek_dataset = Dataset(
    data_path="/home/smachmeier/projects/heiDGA/data/bambenek_dga_domain.csv",
    cast_dataset=cast_bambenek,
)

cic_dataset = Dataset(
    data_path=[
        "/home/smachmeier/projects/heiDGA/example/CICBellDNS2021_CSV_benign.csv",
        "/home/smachmeier/projects/heiDGA/example/CICBellDNS2021_CSV_malware.csv",
        "/home/smachmeier/projects/heiDGA/example/CICBellDNS2021_CSV_phishing.csv",
        "/home/smachmeier/projects/heiDGA/example/CICBellDNS2021_CSV_spam.csv",
    ],
    cast_dataset=cast_cic,
)
