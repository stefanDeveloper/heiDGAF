import pandas as pd
import os

from ydata_profiling import ProfileReport

if __name__ == "__main__":
    if not os.path.exists("./dgta.csv"):
        df_dgta = pd.read_parquet("data/dgta-benchmark.parquet")
        df_dgta.to_csv("dtga.csv")
    else:
        df_dgta = pd.read_csv("./dgta.csv")

    profile = ProfileReport(
        df_dgta, title="DGTA Benchmark Profiling Report", explorative=True
    )
    profile.to_file("dgta_benchmark.html")
