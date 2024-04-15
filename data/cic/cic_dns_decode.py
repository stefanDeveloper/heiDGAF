import re

import polars as pl

if __name__ == "__main__":
    files = [
        "CICBellDNS2021_CSV_benign.csv",
        "CICBellDNS2021_CSV_malware.csv",
        "CICBellDNS2021_CSV_phishing.csv",
        "CICBellDNS2021_CSV_spam.csv",
    ]

    domains = {}
    for file in files:
        with open(f"./{file}") as f:
            domain_file = []
            for line in f:
                txt = line.replace(" ", "")
                x = re.split(",(?![^\[\]]*(?:\])|[^()]*\))", txt)
                if "Domain" != x[4]:
                    domain_file.append(x[4][2:-2])
            domains[file] = domain_file
            print(domain_file[:5])

    for key in domains:
        pl.DataFrame(domains[key]).write_csv(f"{key}_transformed")
