
import math
from string import ascii_lowercase as alc
from typing import List

import polars as pl

from heidgaf.dataset.majestic import MajesticMillionDataset


class Preprocessor():

    def __init__(self, features_to_drop: List):
        """Init.
        
        Args:
            feature_to_drop (list): list of feature to drop
        """
        self.features_to_drop = features_to_drop

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
        # Get letter frequency
        for i in alc:
            x = x.with_columns(
                [
                    (pl.col("query").str.to_lowercase().str.count_matches(rf"{i}").truediv(pl.col("query").str.len_chars())).alias(f"freq_{i}"),
                ]
            )

        x = x.with_columns(
            [   
                # FQDN
                (pl.col("query")).alias("fqdn"),
                (pl.col("query").str.len_chars().alias("fqdn_full_count")),
                (pl.col("query").str.count_matches(r"[a-zA-Z]").truediv(pl.col("query").str.len_chars())).alias("fqdn_alpha_count"),
                (pl.col("query").str.count_matches(r"[0-9]").truediv(pl.col("query").str.len_chars())).alias("fqdn_numeric_count"),
                (pl.col("query").str.count_matches(r"[^\w\s]").truediv(pl.col("query").str.len_chars())).alias("fqdn_special_count"),
            ]
        )
        x = x.with_columns(
            [   
                # Second-level domain
                (pl.when(pl.col("labels").list.len() > 2)
                    .then(
                        pl.col("labels").list.get(-2)
                    ).otherwise(
                        pl.col("labels").list.get(0)
                    ).alias("secondleveldomain"))
            ]
        )
        x = x.with_columns(
            [
                (pl.col("secondleveldomain").str.len_chars().truediv(pl.col("secondleveldomain").str.len_chars()).alias("secondleveldomain_full_count")),
                (pl.col("secondleveldomain").str.count_matches(r"[a-zA-Z]").truediv(pl.col("secondleveldomain").str.len_chars())).alias("secondleveldomainn_alpha_count"),
                (pl.col("secondleveldomain").str.count_matches(r"[0-9]").truediv(pl.col("secondleveldomain").str.len_chars())).alias("secondleveldomainn_numeric_count"),
                (pl.col("secondleveldomain").str.count_matches(r"[^\w\s]").truediv(pl.col("secondleveldomain").str.len_chars())).alias("secondleveldomain_special_count"),
            ]
        )
        x = x.with_columns(
            [   
                # Third-level domain
                (pl.when(pl.col("labels").list.len() > 2)
                    .then(
                    pl.col("labels").list.slice(0, pl.col("labels").list.len() - 2).list.join(".")
                    ).otherwise(pl.lit("")).alias("thirdleveldomain")),
            ]
        )
        x = x.with_columns(
            [
                (pl.col("thirdleveldomain").str.len_chars().truediv(pl.col("thirdleveldomain").str.len_chars()).alias("thirdleveldomain_full_count")),
                (pl.col("thirdleveldomain").str.count_matches(r"[a-zA-Z]").truediv(pl.col("thirdleveldomain").str.len_chars())).alias("thirdleveldomain_alpha_count"),
                (pl.col("thirdleveldomain").str.count_matches(r"[0-9]").truediv(pl.col("thirdleveldomain").str.len_chars())).alias("thirdleveldomain_numeric_count"),
                (pl.col("thirdleveldomain").str.count_matches(r"[^\w\s]").truediv(pl.col("thirdleveldomain").str.len_chars())).alias("thirdleveldomain_special_count"),
            ]
        )
        
        for ent in ["fqdn", "thirdleveldomain", "secondleveldomain"]:
            x = x.with_columns(
                [   
                    (pl.col(ent).map_elements(lambda x: [float(str(x).count(c)) / len(str(x)) for c in dict.fromkeys(list(str(x)))])).alias("prob"),
                ]
            )

            t =  math.log(2.0)

            x = x.with_columns(
                [   
                    # - sum([ p * math.log(p) / math.log(2.0) for p in prob ])
                    (pl.col("prob").list.eval(- pl.element() * pl.element().log() / t).list.sum()).alias(f"{ent}_entropy"),
                ]
            )
            x = x.drop("prob")

        # Fill NaN
        x = x.fill_nan(0)
        # Drop features not useful anymore
        x = x.drop(self.features_to_drop)

        return x
