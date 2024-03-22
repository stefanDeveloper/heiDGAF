import polars as pl

from heidgaf.dataset.majestic import MajesticMillionDataset

class Feature():
    def __init__(self) -> None:
        self.majesticmillion = MajesticMillionDataset()
    
    def lexical_features(self, dataframes: pl.DataFrame) -> pl.DataFrame:
        dataframes = dataframes.with_columns(
            [
                (pl.col("query").str.split(".").alias("labels")),
                (pl.col("query").str.split(".").list.len().alias("label_length")),
                (pl.col("query").str.split(".").list.max().str.len_chars().alias("label_max")),
                (pl.col("query").str.strip_chars(".").str.len_chars().alias("label_average")),
            ]
        )
        
        dataframes = dataframes.with_columns(
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
        
        dataframes = dataframes.with_columns([
            (pl.col("query").entropy(base=2).alias("FQDN_entropy")),
        ])
    
    def majesticmillion_rank_feature():
        pass