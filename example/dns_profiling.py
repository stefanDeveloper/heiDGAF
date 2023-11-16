import pandas as pd
import os
from dns_featuring import fqdn_entropy, count, subdomain, labels, encode_domain

from ydata_profiling import ProfileReport

if __name__ == "__main__":
    if not os.path.exists("./dgta.csv"):
        df_dgta = pd.read_parquet("data/dgta-benchmark.parquet")
        df_dgta = df_dgta.rename(columns={"domain": "Domain"})

        df_dgta = df_dgta.apply(encode_domain, axis=1)
        df_dgta = df_dgta.apply(fqdn_entropy, axis=1)
        df_dgta = df_dgta.apply(count, axis=1)
        df_dgta = df_dgta.apply(labels, axis=1)
        df_dgta = df_dgta.apply(subdomain, axis=1)

        df_dgta.to_csv("dtga.csv")
    else:
        df_dgta = pd.read_csv("./dgta.csv")

    profile = ProfileReport(
        df_dgta, title="DGTA Benchmark Profiling Report", explorative=True
    )
    profile.to_file("dgta_benchmark.html")
