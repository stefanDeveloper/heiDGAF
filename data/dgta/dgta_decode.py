import pandas as pd
import polars as pl


def custom_decode(data):
    retL=[None] * len(data)
    for i, datum in enumerate(data):
        retL[i]=str(datum.decode('latin-1').encode('utf-8').decode('utf-8'))

    return(pl.Series(retL))

if __name__ == "__main__":
    df_dgta = pl.read_parquet("./dgta-benchmark.parquet")
    df_dgta = df_dgta.rename({"domain": "query"})
    df_dgta = df_dgta.with_columns(
        [
            pl.col('query').map(custom_decode)
        ]
    )
    df_dgta.write_csv("dgta.csv")