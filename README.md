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
heidgaf inspect -r data/...
```

Train your own model:

```sh
heidgaf train -m xg -d all
``` 

### Data

Currently, we support the data format scheme provided by the [DNS-Collector](https://github.com/dmachard/go-dnscollector/):

- `{{ .timestamp }}`
- `{{ .return_code }}`
- `{{ .client_ip }}`
- `{{ .server_ip }}`
- `{{ .query }}`
- `{{ .type }}`
- `{{ .answer }}`
- `{{ .size }}b`

For training our models, we rely on the following data sets:

- [CICBellDNS2021](https://www.unb.ca/cic/datasets/dns-2021.html)
- [DGTA Benchmark](https://data.mendeley.com/datasets/2wzf9bz7xr/1)
- [DNS Tunneling Queries for Binary Classification](https://data.mendeley.com/datasets/mzn9hvdcxg/1)
- [UMUDGA - University of Murcia Domain Generation Algorithm Dataset](https://data.mendeley.com/datasets/y8ph45msv8/1)
- [Real-CyberSecurity-Datasets](https://github.com/gfek/Real-CyberSecurity-Datasets/)

However, we compute all feature separately and only rely on the `domain` and `class`.
Currently, we are only interested in binary classification, thus, the `class` is either `benign` or `malicious`.

### Exploratory Data Analysis (EDA)

In the folder `./example` we conducted a Exploratory Data Analysis (EDA) to verify the features of interest for our application.

## Literature

Based on the following work, we implement heiDGAF to find malicious behaviour such as tunneling or data exfiltration in DNS requests.

- EXPOSURE: Finding Malicious Domains Using Passive DNS Analysis

  A passiv DNS pipeline for finding malicious domains using J48 decision tree algorithm.

- Real-Time Detection System for Data Exﬁltration over DNS Tunneling Using Machine Learning

  Propose a hybrid DNS tunneling detection system using Tabu-PIO for feature selection.

- Classifying Malicious Domains using DNS Traffic Analysis
  

- [DeepDGA](https://github.com/roreagan/DeepDGA): Adversarially-Tuned Domain Generation and Detection
  
  DeepDGA detecting (and generating) domains on a per-domain basis which provides a simple and ﬂexible means to detect known DGA families. It uses GANs to bypass detectors and shows the effectiveness of such solutions.

- Kitsune: An Ensemble of Autoencoders for Online Network Intrusion Detection

- SHAP Interpretations of Tree and Neural Network DNS Classifiers for Analyzing DGA Family Characteristics

- [FANCI](https://github.com/fanci-dga-detection/fanci/) : Feature-based Automated NXDomain Classification and Intelligence

### Similar Projects

- [Deep Lookup](https://github.com/ybubnov/deep-lookup/) is a deep learning approach for DNS detection.
- [DGA Detective](https://github.com/COSSAS/dgad) is a temporal convolutional network approach for DNS detection.
- [DGA Detector](https://github.com/Erxathos/DGA-Detector) is a NLP approach for DNS detection.
- [DNS Tunneling Detection](https://github.com/aasthac67/DNS-Tunneling-Detection/)
