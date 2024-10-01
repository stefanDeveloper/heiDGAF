from dataclasses import dataclass
import glob
from typing import Callable, List

import polars as pl
import sklearn.model_selection
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


def cast_dgarchive(data_path: List[str]):
    dataframes = []
    for data in data_path:
        df = pl.read_csv(data, has_header=False, separator=",")
        df = df.rename({"column_1": "query"})
        df = df.select("query")
        df = preprocess(df)
        df = df.with_columns([pl.lit("1").alias("class")])
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


class DatasetLoader:
    def __init__(self) -> None:
        self.dgta_data = Dataset(
            data_path="/home/smachmeier/projects/heiDGA/data/dgta/dgta-benchmark.parquet",
            cast_dataset=cast_dgta,
        )

        self.dga_data = Dataset(
            data_path="/home/smachmeier/projects/heiDGA/data/360_dga_domain.csv",
            cast_dataset=cast_dga,
        )

        self.bambenek_data = Dataset(
            data_path="/home/smachmeier/projects/heiDGA/data/bambenek_dga_domain.csv",
            cast_dataset=cast_bambenek,
        )

        self.cic_data = Dataset(
            data_path=[
                "/home/smachmeier/projects/heiDGA/example/CICBellDNS2021_CSV_benign.csv",
                "/home/smachmeier/projects/heiDGA/example/CICBellDNS2021_CSV_malware.csv",
                "/home/smachmeier/projects/heiDGA/example/CICBellDNS2021_CSV_phishing.csv",
                "/home/smachmeier/projects/heiDGA/example/CICBellDNS2021_CSV_spam.csv",
            ],
            cast_dataset=cast_cic,
        )

        self.dgarchive_data = Dataset(
            data_path=[
                "/home/smachmeier/Downloads/dgarchive/bamital_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/banjori_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/bedep_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/beebone_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/blackhole_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/bobax_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/ccleaner_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/chinad_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/chir_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/conficker_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/corebot_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/cryptolocker_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/darkshell_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/diamondfox_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/dircrypt_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/dmsniff_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/dnsbenchmark_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/dnschanger_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/downloader_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/dyre_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/ebury_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/ekforward_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/emotet_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/feodo_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/fobber_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/gameover_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/gameover_p2p.csv"
                "/home/smachmeier/Downloads/dgarchive/gozi_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/goznym_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/gspy_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/hesperbot_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/infy_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/locky_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/madmax_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/makloader_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/matsnu_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/mirai_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/modpack_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/monerominer_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/murofet_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/murofetweekly_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/mydoom_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/necurs_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/nymaim2_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/nymaim_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/oderoor_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/omexo_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/padcrypt_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/pandabanker_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/pitou_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/proslikefan_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/pushdo_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/pushdotid_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/pykspa2_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/pykspa2s_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/pykspa_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/qadars_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/qakbot_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/qhost_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/qsnatch_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/ramdo_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/ramnit_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/ranbyus_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/randomloader_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/redyms_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/rovnix_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/shifu_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/simda_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/sisron_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/sphinx_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/suppobox_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/sutra_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/symmi_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/szribi_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/tempedreve_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/tempedrevetdd_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/tinba_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/tinynuke_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/tofsee_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/torpig_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/tsifiri_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/ud2_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/ud3_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/ud4_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/urlzone_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/vawtrak_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/vidro_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/vidrotid_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/virut_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/volatilecedar_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/wd_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/xshellghost_dga.csv"
                "/home/smachmeier/Downloads/dgarchive/xxhex_dga.csv"
            ],
            cast_dataset=cast_dgarchive,
        )

    @property
    def dgta_dataset(self) -> Dataset:
        return self.dgta_data

    @property
    def dga_dataset(self) -> Dataset:
        return self.dga_data

    @property
    def bambenek_dataset(self) -> Dataset:
        return self.bambenek_data

    @property
    def cic_dataset(self) -> Dataset:
        return self.cic_data

    @property
    def dgarchive_dataset(self) -> Dataset:
        return self.dgarchive_data


@dataclass
class Dataset:
    """Dataset class."""

    def __init__(
        self,
        data_path: List[str],
        data: pl.DataFrame = None,
        cast_dataset: Callable = None,
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
        self.X_train, self.X_val, self.X_test, self.Y_train, self.Y_val, self.Y_test = (
            self.__train_test_val_split()
        )

    def __len__(self) -> int:
        """Returns the length of data set.

        Returns:
            int: Length of the data set
        """
        return len(self.data)

    def __train_test_val_split(
        self, train_frac: float = 0.8, random_state: int = None
    ) -> tuple[list, list, list, list, list, list]:
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
