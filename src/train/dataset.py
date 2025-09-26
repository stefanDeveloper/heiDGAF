import sys
import os
from dataclasses import dataclass
from typing import Callable, List

import polars as pl
from torch.utils.data.dataset import Dataset

sys.path.append(os.getcwd())
from src.base.log_config import get_logger

logger = get_logger("train.dataset")


def preprocess(x: pl.DataFrame):
    """Preprocesses a `pl.DataFrame` into a basic data set for later transformation.

    Args:
        x (pl.DataFrame): Data sets for preprocessing

    Returns:
        pl.DataFrame: Preprocessed data set
    """
    logger.debug("Start preprocessing data.")
    x = x.filter(pl.col("query").str.len_chars() > 0)
    x = x.unique(subset="query")
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

    logger.debug("Start preprocessing FQDN.")
    x = x.with_columns(
        [
            # FQDN
            (pl.col("query")).alias("fqdn"),
        ]
    )

    x = x.filter(pl.col("labels").list.len().ne(1))

    logger.debug("Start preprocessing Second-level domain.")
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

    logger.debug("Start preprocessing Third-level domain.")
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
    return x


def cast_dga(data_path: str, max_rows: int) -> pl.DataFrame:
    """Cast dga data set.

    Args:
        data_path (str): Data path to data set
        max_rows (int): Maximum rows.

    Returns:
        pl.DataFrame: Loaded pl.DataFrame.
    """
    logger.info(f"Start casting data set {data_path}.")
    df = pl.read_csv(data_path)
    df = df.rename({"Domain": "query"})
    df = df.drop(["DGA_family", "Type"])
    df = df.with_columns([pl.lit("malicious").alias("class")])
    df = preprocess(df)

    # df_legit = df.filter(pl.col("class").eq(0))[:max_rows]
    # df_malicious = df.filter(pl.col("class").eq(1))[:max_rows]

    logger.info(f"Data loaded with shape {df.shape}")
    return df  # pl.concat([df_legit, df_malicious])


def cast_bambenek(data_path: str, max_rows: int) -> pl.DataFrame:
    """Cast Bambenek data set.

    Args:
        data_path (str): Data path to data set
        max_rows (int): Maximum rows.

    Returns:
        pl.DataFrame: Loaded pl.DataFrame.
    """
    logger.info(f"Start casting data set {data_path}.")
    df = pl.read_csv(data_path)
    df = df.rename({"Domain": "query"})
    df = df.drop(["DGA_family", "Type"])
    df = df.with_columns([pl.lit("malicious").alias("class")])
    df = preprocess(df)

    # df_legit = df.filter(pl.col("class").eq(0))[:max_rows]
    # df_malicious = df.filter(pl.col("class").eq(1))[:max_rows]

    logger.info(f"Data loaded with shape {df.shape}")
    return df  # pl.concat([df_legit, df_malicious])


def cast_cic(data_path: List[str], max_rows: int) -> pl.DataFrame:
    """Cast CIC data set.

    Args:
        data_path (str): Data path to data set
        max_rows (int): Maximum rows.

    Returns:
        pl.DataFrame: Loaded pl.DataFrame.
    """
    dataframes = []
    for data in data_path:
        logger.info(f"Start casting data set {data}.")
        y = data.split("_")[-1].split(".")[0]
        df = pl.read_csv(
            data, has_header=False, n_rows=max_rows if max_rows > 0 else None
        )
        if y == "benign":
            df = df.with_columns([pl.lit("legit").alias("class")])
        else:
            df = df.with_columns([pl.lit(y).alias("class")])
        df = df.rename({"column_1": "query"})
        df = preprocess(df)

        logger.info(f"Data loaded with shape {df.shape}")
        dataframes.append(df)

    return pl.concat(dataframes)


def cast_dgarchive(data_path: str, max_rows: int) -> pl.DataFrame:
    """Cast DGArchive data set.

    Args:
        data_path (str): Data path to data set
        max_rows (int): Maximum rows.

    Returns:
        pl.DataFrame: Loaded pl.DataFrame.
    """
    dataframes = []
    logger.info(f"Start casting data set {data_path}.")
    df = pl.read_csv(
        data_path,
        has_header=False,
        separator=",",
        n_rows=max_rows if max_rows > 0 else None,
    )
    df = df.rename({"column_1": "query"})
    df = df.select("query")
    df = df.with_columns(
        [pl.lit(data_path.split("/")[-1].split("_")[0]).alias("class")]
    )
    df = preprocess(df)
    logger.info(f"Data loaded with shape {df.shape}")
    dataframes.append(df)
    return pl.concat(dataframes)


def cast_dgta(data_path: str, max_rows: int) -> pl.DataFrame:
    """Cast DGTA data set.

    Args:
        data_path (str): Data path to data set
        max_rows (int): Maximum rows.

    Returns:
        pl.DataFrame: Loaded pl.DataFrame.
    """

    def __custom_decode(data):
        """Custom decode function.

        Args:
            data (str): Str to decode.

        Returns:
            str: Decoded str.
        """
        return str(data.decode("latin-1").encode("utf-8").decode("utf-8"))

    logger.info(f"Start casting data set {data_path}.")

    df = pl.read_parquet(data_path)
    df = df.rename({"domain": "query"})

    # Drop unnecessary column
    df = df.drop("__index_level_0__")
    df = df.with_columns(
        pl.col("query").map_elements(__custom_decode, return_dtype=pl.Utf8)
    )
    df = preprocess(df)
    # df_legit = df.filter(pl.col("class").eq(0))[:max_rows]
    # df_malicious = df.filter(pl.col("class").eq(1))[:max_rows]

    logger.info(f"Data loaded with shape {df.shape}")
    return df  # pl.concat([df_legit, df_malicious])


def cast_heicloud(data_path: str, max_rows: int) -> pl.DataFrame:
    """Cast heiCLOUD data set.

    Args:
        data_path (str): Data path to data set
        max_rows (int): Maximum rows.

    Returns:
        pl.DataFrame: Loaded pl.DataFrame.
    """
    dataframes = []
    logger.info(f"Start casting data set {data_path}.")
    df = pl.read_csv(
        data_path,
        separator=" ",
        try_parse_dates=False,
        has_header=False,
        n_rows=max_rows if max_rows > 0 else None,
    ).with_columns(
        [
            (pl.col("column_1").str.strptime(pl.Datetime).cast(pl.Datetime)),
        ]
    )
    df = df.rename(
        {
            "column_1": "timestamp",
            "column_2": "return_code",
            "column_3": "src_ip",
            "column_4": "dns_server",
            "column_5": "query",
            "column_6": "type",
            "column_7": "answer",
            "column_8": "size",
        }
    )
    df = df.select("query")
    df = df.with_columns([pl.lit("legit").alias("class")])
    df = preprocess(df)
    logger.info(f"Data loaded with shape {df.shape}")
    dataframes.append(df)
    return pl.concat(dataframes)


class DatasetLoader:
    """DatasetLoader for Training."""

    def __init__(self, base_path: str = "", max_rows: int = -1) -> None:
        """Initialise data sets.

        Args:
            base_path (str, optional): Base path to data set folder. Defaults to "".
            max_rows (int, optional): Maximum rows to consider. Defaults to -1.
        """
        logger.info("Initialise DatasetLoader")
        self.base_path = base_path
        self.max_rows = max_rows
        logger.info("Finished initialisation.")

    @property
    def dgta_dataset(self) -> Dataset:
        self.dgta_data = Dataset(
            name="dgta-benchmark",
            data_path=f"{self.base_path}/dgta/dgta-benchmark.parquet",
            cast_dataset=cast_dgta,
            max_rows=self.max_rows,
        )
        return self.dgta_data

    @property
    def dga_dataset(self) -> Dataset:
        self.dga_data = Dataset(
            name="360_dga_domain",
            data_path=f"{self.base_path}/360_dga_domain.csv",
            cast_dataset=cast_dga,
            max_rows=self.max_rows,
        )
        return self.dga_data

    @property
    def bambenek_dataset(self) -> Dataset:
        self.bambenek_data = Dataset(
            name="bambenek_dga_domain",
            data_path=f"{self.base_path}/bambenek_dga_domain.csv",
            cast_dataset=cast_bambenek,
            max_rows=self.max_rows,
        )
        return self.bambenek_data

    @property
    def heicloud_dataset(self) -> Dataset:
        self.heicloud_data = Dataset(
            name="heicloud",
            data_path=f"{self.base_path}/heicloud/*.txt",
            cast_dataset=cast_heicloud,
            max_rows=self.max_rows,
        )
        return self.heicloud_data

    @property
    def cic_dataset(self) -> Dataset:
        self.cic_data = Dataset(
            name="cic",
            data_path=[
                f"{self.base_path}/cic/CICBellDNS2021_CSV_benign.csv",
                f"{self.base_path}/cic/CICBellDNS2021_CSV_malware.csv",
                f"{self.base_path}/cic/CICBellDNS2021_CSV_phishing.csv",
                f"{self.base_path}/cic/CICBellDNS2021_CSV_spam.csv",
            ],
            cast_dataset=cast_cic,
            max_rows=self.max_rows,
        )
        return self.cic_data

    @property
    def dgarchive_dataset(self) -> list[Dataset]:
        dgarchive_files = [
            f for f in os.listdir(f"{self.base_path}/dgarchive") if f.endswith(".csv")
        ]
        self.dgarchive_data = []
        for dgarchive_file in dgarchive_files:
            self.dgarchive_data.append(
                Dataset(
                    name=f"dgarchive_{dgarchive_file.split('.')[0]}",
                    data_path=f"{self.base_path}/dgarchive/{dgarchive_file}",
                    cast_dataset=cast_dgarchive,
                    max_rows=10000,  # self.max_rows,
                )
            )
        return self.dgarchive_data


@dataclass
class Dataset:
    """Dataset class."""

    def __init__(
        self,
        name: str,
        data_path: List[str],
        data: pl.DataFrame = None,
        cast_dataset: Callable = None,
        max_rows: int = -1,
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
        self.name = name
        self.data_path = data_path
        if cast_dataset != None and data_path != "":
            logger.info("Cast function provided, load data set.")
            self.data: pl.DataFrame = cast_dataset(data_path, max_rows)
        elif data_path != "":
            logger.info("Data path provided, load data set.")
            self.data: pl.DataFrame = pl.read_csv(data_path)
        elif not data is None:
            logger.info("Data set provided, load data set.")
            self.data: pl.DataFrame = data
        else:
            logger.error("No data given!")
            raise NotImplementedError("No data given")

    def __len__(self) -> int:
        """Returns the length of data set.

        Returns:
            int: Length of the data set
        """
        return len(self.data)
