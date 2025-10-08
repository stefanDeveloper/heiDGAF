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

![Pipeline overview](https://raw.githubusercontent.com/stefanDeveloper/heiDGAF/main/docs/media/heidgaf_overview_detailed.drawio.png?raw=true)

## Getting Started

##### Run **heiDGAF** using Docker Compose:

```sh
HOST_IP=127.0.0.1 docker compose -f docker/docker-compose.yml up
```
<p align="center">
  <img src="https://raw.githubusercontent.com/stefanDeveloper/heiDGAF/main/assets/terminal_example.gif?raw=true" alt="Terminal example"/>
</p>

##### Or run the modules locally on your machine:
```sh
python -m venv .venv
source .venv/bin/activate

sh install_requirements.sh
```
Alternatively, you can use `pip install` and enter all needed requirements individually with `-r requirements.*.txt`.

Now, you can start each stage, e.g. the inspector:

```sh
python src/inspector/inspector.py
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>


## Usage

### Configuration

To configure **heiDGAF** according to your needs, use the provided `config.yaml`.

The most relevant settings are related to your specific log line format, the model you want to use, and
possibly infrastructure.

The section `pipeline.log_collection.collector.logline_format` has to be adjusted to reflect your specific input log
line format. Using our adjustable and flexible log line configuration, you can rename, reorder and fully configure each
field of a valid log line. Freely define timestamps, RegEx patterns, lists, and IP addresses. For example, your
configuration might look as follows:

```yml
- [ "timestamp", Timestamp, "%Y-%m-%dT%H:%M:%S.%fZ" ]
- [ "status_code", ListItem, [ "NOERROR", "NXDOMAIN" ], [ "NXDOMAIN" ] ]
- [ "client_ip", IpAddress ]
- [ "dns_server_ip", IpAddress ]
- [ "domain_name", RegEx, '^(?=.{1,253}$)((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,63}$' ]
- [ "record_type", ListItem, [ "A", "AAAA" ] ]
- [ "response_ip", IpAddress ]
- [ "size", RegEx, '^\d+b$' ]
```

The options `pipeline.data_inspection` and `pipeline.data_analysis` are relevant for configuring the model. The section
`environment` can be fine-tuned to prevent naming collisions for Kafka topics and adjust addressing in your environment.

For more in-depth information on your options, have a look at our
[official documentation](https://heidgaf.readthedocs.io/en/latest/usage.html), where we provide tables explaining all
values in detail.

### Monitoring
To monitor the system and observe its real-time behavior, multiple Grafana dashboards have been set up.

Have a look at the following pictures showing examples of how these dashboards might look at runtime.

<details>
  <summary><strong>Overview</strong> dashboard</summary>

  Contains the most relevant information on the system's runtime behavior, its efficiency and its effectivity.

  <p align="center">
    <a href="./assets/readme_assets/overview.png">
      <img src="./assets/readme_assets/overview.png" alt="Overview Dashboard" width="90%"/>
    </a>
  </p>

</details>

<details>
  <summary><strong>Latencies</strong> dashboard</summary>

  Presents any information on latencies, including comparisons between the modules and more detailed,
  stand-alone metrics.

  <p align="center">
    <a href="./assets/readme_assets/latencies.jpeg">
      <img src="./assets/readme_assets/latencies.jpeg" alt="Latencies Dashboard" width="90%"/>
    </a>
  </p>

</details>

<details>
  <summary><strong>Log Volumes</strong> dashboard</summary>

  Presents any information on the fill levels of each module, i.e. the number of entries that are currently in the
  module for processing. Includes comparisons between the modules, more detailed, stand-alone metrics, as well as
  total numbers of logs entering the pipeline or being marked as fully processed.

  <p align="center">
    <a href="./assets/readme_assets/log_volumes.jpeg">
      <img src="./assets/readme_assets/log_volumes.jpeg" alt="Log Volumes Dashboard" width="90%"/>
    </a>
  </p>

</details>

<details>
  <summary><strong>Alerts</strong> dashboard</summary>

  Presents details on the number of logs detected as malicious including IP addresses responsible for those alerts.

  <p align="center">
    <a href="./assets/readme_assets/alerts.png">
      <img src="./assets/readme_assets/alerts.png" alt="Alerts Dashboard" width="90%"/>
    </a>
  </p>

</details>

<details>
  <summary><strong>Dataset</strong> dashboard</summary>

  This dashboard is only active for the **_datatest_** mode. Users who want to test their own models can use this mode
  for inspecting confusion matrices on testing data.

  > [!CAUTION]
  > This feature is in a very early development stage.

  <p align="center">
    <a href="./assets/readme_assets/datatests.png">
      <img src="./assets/readme_assets/datatests.png" alt="Dataset Dashboard" width="80%"/>
    </a>
  </p>

</details>

<p align="right">(<a href="#readme-top">back to top</a>)</p>


## Models and Training

To train and test our and possibly your own models, we currently rely on the following datasets:

- [CICBellDNS2021](https://www.unb.ca/cic/datasets/dns-2021.html)
- [DGTA Benchmark](https://data.mendeley.com/datasets/2wzf9bz7xr/1)
- [DNS Tunneling Queries for Binary Classification](https://data.mendeley.com/datasets/mzn9hvdcxg/1)
- [UMUDGA - University of Murcia Domain Generation Algorithm Dataset](https://data.mendeley.com/datasets/y8ph45msv8/1)
- [DGArchive](https://dgarchive.caad.fkie.fraunhofer.de/)

We compute all features separately and only rely on the `domain` and `class` for binary classification.

### Inserting Data for Testing

For testing purposes, we provide multiple scripts in the `scripts` directory. Use `real_logs.dev.py` to send data from
the datasets into the pipeline. After downloading the dataset and storing it under `<project-root>/data`, run
```sh
python scripts/real_logs.dev.py
```
to start continuously inserting dataset traffic.

### Training Your Own Models

> [!IMPORTANT]
> This is only a brief wrap-up of a custom training process.
> We highly encourage you to have a look at the [documentation](https://heidgaf.readthedocs.io/en/latest/training.html)
> for a full description and explanation of the configuration parameters.

We feature two trained models:
1. XGBoost (`src/train/model.py#XGBoostModel`) and
2. RandomForest (`src/train/model.py#RandomForestModel`).

After installing the requirements, use `src/train/train.py`:

```sh
> python -m venv .venv
> source .venv/bin/activate

> pip install -r requirements/requirements.train.txt

> python src/train/train.py
Usage: train.py [OPTIONS] COMMAND [ARGS]...

Options:
  -h, --help  Show this message and exit.

Commands:
  explain
  test
  train
```

Setting up the [dataset directories](#insert-test-data) (and adding the code for your model class if applicable) lets you start
the training process by running the following commands:

##### Model Training

```sh
> python src/train/train.py train  --dataset <dataset_type> --dataset_path <path/to/your/datasets> --model <model_name>
```
The results will be saved per default to `./results`, if not configured otherwise.

##### Model Tests

```sh
> python src/train/train.py test  --dataset <dataset_type> --dataset_path <path/to/your/datasets> --model <model_name> --model_path <path_to_model_version>
```

##### Model Explain

```sh
> python src/train/train.py explain  --dataset <dataset_type> --dataset_path <path/to/your/datasets> --model <model_name> --model_path <path_to_model_version>
```
This will create a `rules.txt` file containing the innards of the model, explaining the rules it created.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


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
