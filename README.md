# heiDGAF - DGA Finder

> ML based DNS analyzer to detect Domain Generation Algorithms (DGAs) tunneling, and data exfiltration of malicious actors.

## Getting Started

```sh
python -m venv .venv
pip install .

heidgaf -h
```

Run your analysis:

```sh
heidgaf process start -r data/...
```

### Data

Currently, we support the data format scheme:

`{{ .timestamp }} {{ .return_code }} {{ .client_ip }} {{ .server_ip }} {{ .query }} {{ .type }} {{ .answer }} {{ .size }}b`

For training our models, we rely on the following data sets:

- CICBellDNS2021
- DGTA Benchmark
- Majestic Million

### Exploratory Data Analysis (EDA)

In the folder `./example` we conducted a Exploratory Data Analysis (EDA) to verify the features of interest for our application.

## Literature

Based on the following work we implement heiDGAF to find malicious behaviour in DNS request.

- EXPOSURE: Finding Malicious Domains Using Passive DNS Analysis
- 
