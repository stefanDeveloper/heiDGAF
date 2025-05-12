<a id="readme-top"></a>

<!-- PROJECT SHIELDS -->
<div align="center">

[![Codecov Coverage][coverage-shield]][coverage-url]
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![EUPL License][license-shield]][license-url]


</div>

<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://github.com/stefanDeveloper/heiDGAF">
    <img src="https://raw.githubusercontent.com/stefanDeveloper/heiDGAF/main/assets/heidgaf_logo_normal.png?raw=true" alt="Logo">
  </a>

<h3 align="center">heiDGAF - Domain Generation Algorithms Finder</h3>

  <p align="center">
    Machine learning-based DNS classifier for detecting Domain Generation Algorithms (DGAs), tunneling, and data exfiltration by malicious actors.
    <br />
    <a href="https://heidgaf.readthedocs.io/en/latest/"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://mybinder.org/v2/gh/stefanDeveloper/heiDGAF-tutorials/HEAD?labpath=demo_notebook.ipynb">View Demo</a>
    ·
    <a href="https://github.com/stefanDeveloper/heiDGAF/issues/new?labels=bug&template=bug-report---.md">Report Bug</a>
    ·
    <a href="https://github.com/stefanDeveloper/heiDGAF/issues/new?labels=enhancement&template=feature-request---.md">Request Feature</a>
  </p>
</div>

> [!CAUTION]
> The project is under active development right now. Everything might change, break, or move around quickly.

<table>
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

## About the Project

![Pipeline overview](https://raw.githubusercontent.com/stefanDeveloper/heiDGAF/main/docs/media/pipeline_overview.png?raw=true)

## Getting Started

If you want to use heiDGAF, just use the provided Docker compose to quickly bootstrap your environment:

```
docker compose -f docker/docker-compose.yml up
```

![Terminal example](https://raw.githubusercontent.com/stefanDeveloper/heiDGAF/main/assets/terminal_example.gif?raw=true)

### Configuration

The following table lists the most important configuration parameters with their default values.
The configuration options can be set in the ![config.yaml](./config.yaml) in the root directory.

| Path                                       | Description                                                                 | Default Value                                                                                                |
| :----------------------------------------- | :-------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------- |
| **logging**                                | Global and module-specific logging configurations.                          |                                                                                                              |
| `logging.base.debug`                       | Default debug logging level for all modules if not overridden.              | `false`                                                                                                      |
| `logging.modules.<module_name>.debug`      | Specific debug logging level for a given module (e.g., `log_storage.logserver`). | `false` (for all listed modules)                                                                             |
| **pipeline**                               | Configuration for the data processing pipeline stages.                      |                                                                                                              |
| `pipeline.log_storage.logserver.input_file` | Path to the input file for the log server.                                  | `"/opt/file.txt"`                                                                                            |
| `pipeline.log_collection.collector.logline_format` | Defines the format of incoming log lines, specifying field name, type, and parsing rules/values. | Array of field definitions (e.g., `["timestamp", Timestamp, "%Y-%m-%dT%H:%M:%S.%fZ"]`)                     |
| `pipeline.log_collection.batch_handler.batch_size` | Number of log lines to collect before sending a batch.                      | `10000`                                                                                                      |
| `pipeline.log_collection.batch_handler.batch_timeout` | Maximum time (in seconds) to wait before sending a partially filled batch.  | `30.0`                                                                                                       |
| `pipeline.log_collection.batch_handler.subnet_id.ipv4_prefix_length` | IPv4 prefix length for subnet identification.                             | `24`                                                                                                         |
| `pipeline.log_collection.batch_handler.subnet_id.ipv6_prefix_length` | IPv6 prefix length for subnet identification.                             | `64`                                                                                                         |
| `pipeline.data_inspection.inspector.mode`  | Mode of operation for the data inspector.                                   | `univariate` (options: `multivariate`, `ensemble`)                                                           |
| `pipeline.data_inspection.inspector.ensemble.model` | Model to use when inspector mode is `ensemble`.                             | `WeightEnsemble`                                                                                             |
| `pipeline.data_inspection.inspector.ensemble.module` | Python module for the ensemble model.                                       | `streamad.process`                                                                                           |
| `pipeline.data_inspection.inspector.ensemble.model_args` | Arguments for the ensemble model.                                           | (empty by default) TODO: describe in more detail                                                                                           |
| `pipeline.data_inspection.inspector.models` | List of models to use for data inspection (e.g., anomaly detection).      | Array of model definitions (e.g., `{"model": "ZScoreDetector", "module": "streamad.model", "model_args": {"is_global": false}}`) TODO: more details!|
| `pipeline.data_inspection.inspector.anomaly_threshold` | Threshold for classifying an observation as an anomaly.                     | `0.01`   TODO: elaborate                                                                                                    |
| `pipeline.data_inspection.inspector.score_threshold` | Threshold for the anomaly score.                                            | `0.5`               <TODO: elaborate>                                                                                         |
| `pipeline.data_inspection.inspector.time_type` | Unit of time used in time range calculations.                               | `ms`                                                                                                         |
| `pipeline.data_inspection.inspector.time_range` | Time range for inspection.                                                  | `20`                                                                                                         |
| `pipeline.data_analysis.detector.model`    | Model to use for data analysis (e.g., DGA detection).                       | `rf` (Random Forest) option: `XGBoost`                                                    |
| `pipeline.data_analysis.detector.checksum` | Checksum for the model file to ensure integrity.                            | `ba1f718179191348fe2abd51644d76191d42a5d967c6844feb3371b6f798bf06`    TODO: elaborate                                       |
| `pipeline.data_analysis.detector.base_url` | Base URL for downloading the model if not present locally.                  | `https://heibox.uni-heidelberg.de/d/0d5cbcbe16cd46a58021/`                                                  |
| `pipeline.data_analysis.detector.threshold` | Threshold for the detector's classification.                                | `0.5`          TODO elaborate                                                                                              |
| `pipeline.monitoring.clickhouse_connector.batch_size` | Batch size for sending data to ClickHouse.                                | `50`                                                                                                         |
| `pipeline.monitoring.clickhouse_connector.batch_timeout` | Batch timeout (in seconds) for sending data to ClickHouse.                | `2.0`                                                                                                        |
| **environment**                            | Configuration for external services and infrastructure.                     |                                                                                                              |
| `environment.kafka_brokers`                | List of Kafka broker hostnames and ports.                                   | `[{"hostname": "kafka1", "port": 8097}, {"hostname": "kafka2", "port": 8098}, {"hostname": "kafka3", "port": 8099}]` |
| `environment.kafka_topics.pipeline.<topic_name>` | Kafka topic names for various stages in the pipeline.                     | e.g., `logserver_in: "pipeline-logserver_in"`                                                                |
| `environment.monitoring.clickhouse_server.hostname` | Hostname of the ClickHouse server for monitoring data.                      | `clickhouse-server`                                                                                          |


### Developing

> [!IMPORTANT]
> More information will be added soon! Go and watch the repository for updates.

Install all Python requirements:

```sh
python -m venv .venv
source .venv/bin/activate

sh install_requirements.sh
```

Alternatively, you can use `pip install` and enter all needed requirements individually with `-r requirements.*.txt`.

Now, you can start each stage, e.g. the inspector:

```sh
python src/inspector/main.py
```

### Train your own models

> [!IMPORTANT]
> More information will be added soon! Go and watch the repository for updates.

Currently, we enable two trained models, namely XGBoost and RandomForest.

```sh
python -m venv .venv
source .venv/bin/activate

pip install -r requirements/requirements.train.txt
```

For training our models, we rely on the following data sets:

- [CICBellDNS2021](https://www.unb.ca/cic/datasets/dns-2021.html)
- [DGTA Benchmark](https://data.mendeley.com/datasets/2wzf9bz7xr/1)
- [DNS Tunneling Queries for Binary Classification](https://data.mendeley.com/datasets/mzn9hvdcxg/1)
- [UMUDGA - University of Murcia Domain Generation Algorithm Dataset](https://data.mendeley.com/datasets/y8ph45msv8/1)
- [Real-CyberSecurity-Datasets](https://github.com/gfek/Real-CyberSecurity-Datasets/)

However, we compute all feature separately and only rely on the `domain` and `class`.
Currently, we are only interested in binary classification, thus, the `class` is either `benign` or `malicious`.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


To retrain the existing models you can simply run: 
```
echo "adapt me!"
```


### Data

> [!IMPORTANT]
> We support custom schemes.

```yml
loglines:
  fields:
    - [ "timestamp", RegEx, '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$' ]
    - [ "status_code", ListItem, [ "NOERROR", "NXDOMAIN" ], [ "NXDOMAIN" ] ]
    - [ "client_ip", IpAddress ]
    - [ "dns_server_ip", IpAddress ]
    - [ "domain_name", RegEx, '^(?=.{1,253}$)((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,63}$' ]
    - [ "record_type", ListItem, [ "A", "AAAA" ] ]
    - [ "response_ip", IpAddress ]
    - [ "size", RegEx, '^\d+b$' ]
```

<!-- CONTRIBUTING -->

## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any
contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also
simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

### Top contributors:

<a href="https://github.com/stefanDeveloper/heiDGAF/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=stefanDeveloper/heiDGAF" alt="contrib.rocks image" />
</a>


<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- LICENSE -->

## License

Distributed under the EUPL License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->

[contributors-shield]: https://img.shields.io/github/contributors/stefanDeveloper/heiDGAF.svg?style=for-the-badge

[contributors-url]: https://github.com/stefanDeveloper/heiDGAF/graphs/contributors

[forks-shield]: https://img.shields.io/github/forks/stefanDeveloper/heiDGAF.svg?style=for-the-badge

[forks-url]: https://github.com/stefanDeveloper/heiDGAF/network/members

[stars-shield]: https://img.shields.io/github/stars/stefanDeveloper/heiDGAF.svg?style=for-the-badge

[stars-url]: https://github.com/stefanDeveloper/heiDGAF/stargazers

[issues-shield]: https://img.shields.io/github/issues/stefanDeveloper/heiDGAF.svg?style=for-the-badge

[issues-url]: https://github.com/stefanDeveloper/heiDGAF/issues

[license-shield]: https://img.shields.io/github/license/stefanDeveloper/heiDGAF.svg?style=for-the-badge

[license-url]: https://github.com/stefanDeveloper/heiDGAF/blob/master/LICENSE.txt

[coverage-shield]: https://img.shields.io/codecov/c/github/stefanDeveloper/heiDGAF?style=for-the-badge

[coverage-url]: https://app.codecov.io/github/stefanDeveloper/heiDGAF
