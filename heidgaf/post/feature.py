
import polars as pl

from typing import List

from heidgaf.dataset.majestic import MajesticMillionDataset

class Preprocessor():

    def __init__(self, features_to_drop: List):
        """Init.
        
        Args:
            feature_to_drop (list): list of feature to drop
        """
        self.features_to_drop = features_to_drop
        self.majesticmillion = MajesticMillionDataset()

    def transform(self, x: pl.DataFrame) -> pl.DataFrame:
        """Transform our dataset with new features
        
        Args:
            x (pl.DataFrame): dataframe with our features
        
        Returns:
            pl.DataFrame: preprocessed dataframe
        """
        x = x.with_columns(
            [
                (pl.col("query").str.split(".").alias("labels")),
                (pl.col("query").str.split(".").list.len().alias("label_length")),
                (pl.col("query").str.split(".").list.max().str.len_chars().alias("label_max")),
                (pl.col("query").str.strip_chars(".").str.len_chars().alias("label_average")),
            ]
        )

        x = x.with_columns(
            [
                # FQDN
                (pl.when(pl.col("labels").list.len() > 2)
                    .then(
                        pl.col("labels").list.get(-2)
                    ).otherwise(
                        pl.col("labels").list.get(0)
                    ).alias("SLD")),
                (pl.col("query").str.len_chars().alias("FQDN_full_count")),
                (pl.col("query").str.count_matches(r"[0-9]").alias("FQDN_numeric_count")),
                (pl.col("query").str.count_matches(r"[^\w\s]").alias("FQDN_special_count")),
                # Third-level domain
                (pl.when(pl.col("labels").list.len() > 2)
                    .then(
                        pl.col("labels").list.get(0).str.len_chars()
                    ).otherwise(0).alias("SD_full_count")),
                (pl.when(pl.col("labels").list.len() > 2)
                    .then(
                        pl.col("labels").list.get(0).str.count_matches(r"[0-9]")
                    ).otherwise(0).alias("SD_numeric_count")),
                (pl.when(pl.col("labels").list.len() > 2)
                    .then(
                        pl.col("labels").list.get(0).str.count_matches(r"[^\w\s]")
                    ).otherwise(0).alias("SD_special_count")),
                # Second-level domain
                (pl.when(pl.col("labels").list.len() > 2)
                    .then(
                        pl.col("labels").list.get(1).str.len_chars()
                    ).otherwise(
                        pl.col("labels").list.get(0).str.len_chars()
                    ).alias("SLD_full_count")),
                (pl.when(pl.col("labels").list.len() > 2)
                    .then(
                        pl.col("labels").list.get(1).str.count_matches(r"[0-9]")
                    ).otherwise(
                        pl.col("labels").list.get(0).str.count_matches(r"[0-9]")
                    ).alias("SLD_numeric_count")),
                (pl.when(pl.col("labels").list.len() > 2)
                    .then(
                        pl.col("labels").list.get(1).str.count_matches(r"[^\w\s]")
                    ).otherwise(
                        pl.col("labels").list.get(0).str.count_matches(r"[^\w\s]")
                    ).alias("SLD_special_count")),
            ]
        )
        
        x = x.with_columns([
            (pl.col("query").entropy(base=2).alias("FQDN_entropy")),
        ])
        
        # Drop features not useful anymore
        x = x.drop(self.features_to_drop)

        return x
