import os
import sys
from string import ascii_lowercase as alc
from typing import List

import math
import polars as pl

sys.path.append(os.getcwd())
from src.base.log_config import get_logger

logger = get_logger("train.feature")


class Processor:
    """Extracts statistical and linguistic features from domain name datasets.

    Computes comprehensive feature sets including domain label statistics, character
    frequencies, entropy measures, and domain structure analysis for machine learning
    model training and DGA detection tasks.
    """

    def __init__(self, features_to_drop: List) -> None:
        """
        Args:
            features_to_drop (List): List of column names to exclude from final features.
        """
        self.features_to_drop = features_to_drop

    def transform(self, x: pl.DataFrame) -> pl.DataFrame:
        """Extracts comprehensive feature set from domain name dataset.

        Computes domain label statistics, character frequencies for all letters,
        character type ratios, and entropy measures for different domain levels.
        Handles missing values and removes specified columns from final output.

        Args:
            x (pl.DataFrame): Input dataset with domain structure columns.

        Returns:
            pl.DataFrame: Feature-engineered dataset ready for ML model training.
        """
        logger.debug("Start data transformation")
        x = x.with_columns(
            [
                (pl.col("query").str.split(".").list.len().alias("label_length")),
                (
                    pl.col("query")
                    .str.split(".")
                    .list.max()
                    .str.len_chars()
                    .alias("label_max")
                ),
                (
                    pl.col("query")
                    .str.strip_chars(".")
                    .str.len_chars()
                    .alias("label_average")
                ),
            ]
        )
        logger.debug("Get letter frequency")
        for i in alc:
            x = x.with_columns(
                [
                    (
                        pl.col("query")
                        .str.to_lowercase()
                        .str.count_matches(rf"{i}")
                        .truediv(pl.col("query").str.len_chars())
                    ).alias(f"freq_{i}"),
                ]
            )
        logger.debug("Get full, alpha, special, and numeric count.")
        for level in ["thirdleveldomain", "secondleveldomain", "fqdn"]:
            x = x.with_columns(
                [
                    (
                        pl.when(pl.col(level).str.len_chars().eq(0))
                        .then(pl.lit(0))
                        .otherwise(
                            pl.col(level)
                            .str.len_chars()
                            .truediv(pl.col(level).str.len_chars())
                        )
                    ).alias(f"{level}_full_count"),
                    (
                        pl.when(pl.col(level).str.len_chars().eq(0))
                        .then(pl.lit(0))
                        .otherwise(
                            pl.col(level)
                            .str.count_matches(r"[a-zA-Z]")
                            .truediv(pl.col(level).str.len_chars())
                        )
                    ).alias(f"{level}_alpha_count"),
                    (
                        pl.when(pl.col(level).str.len_chars().eq(0))
                        .then(pl.lit(0))
                        .otherwise(
                            pl.col(level)
                            .str.count_matches(r"[0-9]")
                            .truediv(pl.col(level).str.len_chars())
                        )
                    ).alias(f"{level}_numeric_count"),
                    (
                        pl.when(pl.col(level).str.len_chars().eq(0))
                        .then(pl.lit(0))
                        .otherwise(
                            pl.col(level)
                            .str.count_matches(r"[^\w\s]")
                            .truediv(pl.col(level).str.len_chars())
                        )
                    ).alias(f"{level}_special_count"),
                ]
            )

        logger.debug("Start entropy calculation")
        for ent in ["fqdn", "thirdleveldomain", "secondleveldomain"]:
            x = x.with_columns(
                [
                    (
                        pl.col(ent).map_elements(
                            lambda x: [
                                float(str(x).count(c)) / len(str(x))
                                for c in dict.fromkeys(list(str(x)))
                            ],
                            return_dtype=pl.List(pl.Float64),
                        )
                    ).alias("prob"),
                ]
            )

            t = math.log(2.0)

            x = x.with_columns(
                [
                    (
                        pl.col("prob")
                        .list.eval(-pl.element() * pl.element().log() / t)
                        .list.sum()
                    ).alias(f"{ent}_entropy"),
                ]
            )
            x = x.drop("prob")
        logger.debug("Finished entropy calculation")

        logger.debug("Fill NaN.")
        x = x.fill_nan(0)

        logger.debug("Drop features that are not useful.")
        x = x.drop(self.features_to_drop)

        logger.debug("Finished data transformation")

        logger.info("Finished data transformation")

        return x
