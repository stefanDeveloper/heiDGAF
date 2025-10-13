import os
import sys
from dataclasses import dataclass
from typing import Callable, List

import polars as pl
from torch.utils.data.dataset import Dataset

sys.path.append(os.getcwd())
from src.base.log_config import get_logger

logger = get_logger("train.dataset")


def preprocess(x: pl.DataFrame) -> pl.DataFrame:
    """Preprocesses DataFrame into structured dataset for feature extraction.

    Filters out empty queries, removes duplicates, splits domain names into labels,
    and extracts top-level domain (TLD), second-level domain, and third-level domain
    components for further analysis.

    Args:
        x (pl.DataFrame): Raw dataset containing DNS queries for preprocessing.

    Returns:
        pl.DataFrame: Preprocessed dataset with structured domain components.
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
    """Loads and processes DGA dataset from CSV file.

    Reads DGA domain dataset, renames columns to standard format, adds malicious
    class label, and applies preprocessing to structure domain components.

    Args:
        data_path (str): Path to the DGA dataset CSV file.
        max_rows (int): Maximum number of rows to process.

    Returns:
        pl.DataFrame: Processed DGA dataset with structured domain information.
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
    """Loads and processes Bambenek DGA dataset from CSV file.

    Reads Bambenek DGA domain dataset, renames columns to standard format, adds
    malicious class label, and applies preprocessing to structure domain components.

    Args:
        data_path (str): Path to the Bambenek dataset CSV file.
        max_rows (int): Maximum number of rows to process.

    Returns:
        pl.DataFrame: Processed Bambenek dataset with structured domain information.
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
    """Loads and processes CIC DNS dataset from multiple CSV files.

    Reads CIC DNS datasets (benign, malware, phishing, spam), assigns appropriate
    class labels based on filename, and combines all datasets into a unified format.

    Args:
        data_path (List[str]): List of paths to CIC dataset CSV files.
        max_rows (int): Maximum number of rows to process per file.

    Returns:
        pl.DataFrame: Combined CIC dataset with structured domain information.
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
    """Loads and processes DGArchive dataset from CSV file.

    Reads DGArchive domain dataset, extracts class label from filename, renames
    columns to standard format, and applies preprocessing for domain analysis.

    Args:
        data_path (str): Path to the DGArchive dataset CSV file.
        max_rows (int): Maximum number of rows to process.

    Returns:
        pl.DataFrame: Processed DGArchive dataset with structured domain information.
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
    """Loads and processes DGTA benchmark dataset from Parquet file.

    Reads DGTA benchmark dataset, handles custom UTF-8 encoding, renames columns
    to standard format, and applies preprocessing for domain structure analysis.

    Args:
        data_path (str): Path to the DGTA dataset Parquet file.
        max_rows (int): Maximum number of rows to process.

    Returns:
        pl.DataFrame: Processed DGTA dataset with structured domain information.
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
    """Loads and processes heiCLOUD dataset from space-separated text file.

    Reads heiCLOUD DNS log dataset, parses space-separated columns, extracts
    domain queries, and labels them as legitimate traffic for training.

    Args:
        data_path (str): Path to the heiCLOUD dataset text file.
        max_rows (int): Maximum number of rows to process.

    Returns:
        pl.DataFrame: Processed heiCLOUD dataset with legitimate domain labels.
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
            "column_3": "client_ip",
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
    """Manages loading and access to multiple DNS datasets for training.

    Provides convenient access to various DNS datasets including DGA detection
    benchmarks, legitimate traffic datasets, and combined multi-source datasets.
    Handles dataset-specific loading and preprocessing requirements.
    """

    def __init__(self, base_path: str = "", max_rows: int = -1) -> None:
        """
        Args:
            base_path (str): Base directory path containing all dataset folders.
            max_rows (int): Maximum rows to load per dataset (default: -1 for unlimited).
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
    """Single DNS dataset with loading and preprocessing capabilities

    Encapsulates dataset information including name, file paths, and data processing
    functions. Supports flexible data loading from various sources including CSV,
    Parquet, and text files with custom preprocessing functions.
    """

    def __init__(
        self,
        name: str,
        data_path: List[str],
        data: pl.DataFrame = None,
        cast_dataset: Callable = None,
        max_rows: int = -1,
    ) -> None:
        """
        Loads dataset either from file path using optional preprocessing function
        or directly from provided DataFrame. Supports various data formats and
        custom preprocessing callbacks for dataset-specific requirements.

        Args:
            name (str): Unique identifier for the dataset.
            data_path (List[str]): File paths to dataset files.
            data (pl.DataFrame): Pre-loaded dataset (alternative to data_path).
            cast_dataset (Callable): Custom preprocessing function for data loading.
            max_rows (int): Maximum rows to load (default: -1 for unlimited).

        Raises:
            NotImplementedError: When neither data_path nor data is provided.
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
        """Returns number of rows in the dataset.

        Returns:
            int: Total number of records in the dataset.
        """
        return len(self.data)
