# heiDGAF - Domain Generation Algorithms Finder

> ML based DNS analyzer to detect Domain Generation Algorithms (DGAs) tunneling, and data exfiltration of malicious actors.


<table>
<tr>
  <td><b>Live Notebook</b></td>
  <td>
    <a href="https://mybinder.org/v2/gh/stefanDeveloper/heiDGAF-tutorials/HEAD?labpath=demo_notebook.ipynb">
    <img src="https://img.shields.io/badge/notebook-launch-blue?logo=jupyter&style=for-the-badge" alt="live notebook" />
    </a>
  </td>
</tr>
<tr>
  <td><b>Latest Release</b></td>
  <td>
    <a href="https://pypi.python.org/pypi/heiDGAF">
    <img src="https://img.shields.io/pypi/v/heiDGAF.svg?logo=pypi&style=for-the-badge" alt="latest release" />
    </a>
  </td>
</tr>

<tr>
  <td><b>Supported Versions</b></td>
  <td>
    <a href="https://pypi.org/project/heiDGAF/">
    <img src="https://img.shields.io/pypi/pyversions/heiDGAF?logo=python&style=for-the-badge" alt="python3" />
    </a>
  </td>
</tr>
<tr>
  <td><b>Project License</b></td>
  <td>
    <a href="https://github.com/stefanDeveloper/heiDGAF/blob/main/LICENSE">
    <img src="https://img.shields.io/pypi/l/heiDGAF?logo=gnu&style=for-the-badge&color=blue" alt="License" />
    </a>
  </td>
</tr>
<tr>
  <td><b>Continuous Integration</b></td>
  <td>
    <a href="https://github.com/stefanDeveloper/heiDGAF/actions/workflows/build_test_linux.yml">
    <img src="https://img.shields.io/github/actions/workflow/status/stefanDeveloper/heiDGAF/build_test_linux.yml?branch=main&logo=linux&style=for-the-badge&label=linux" alt="Linux WorkFlows" />
    </a>
    <a href="https://github.com/stefanDeveloper/heiDGAF/actions/workflows/build_test_macos.yml">
    <img src="https://img.shields.io/github/actions/workflow/status/stefanDeveloper/heiDGAF/build_test_macos.yml?branch=main&logo=apple&style=for-the-badge&label=macos" alt="MacOS WorkFlows" />
    </a>
    <a href="https://github.com/stefanDeveloper/heiDGAF/actions/workflows/build_test_windows.yml">
    <img src="https://img.shields.io/github/actions/workflow/status/stefanDeveloper/heiDGAF/build_test_windows.yml?branch=main&logo=windows&style=for-the-badge&label=windows" alt="Windows WorkFlows" />
    </a>
  </td>
</tr>
</table>

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
