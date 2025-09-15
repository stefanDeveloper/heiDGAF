# Benchmark Test Runner
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /usr/src/app

RUN pip --disable-pip-version-check install --no-cache-dir --no-compile marshmallow_dataclass colorlog pyYAML confluent_kafka numpy polars scikit-learn torch progressbar2

COPY src/base ./src/base
COPY src/train ./src/train
COPY config.yaml .
COPY benchmarking/test_runner ./benchmarking/test_runner
COPY benchmarking/sql_queries/entering_processed/activity_last_three_minutes.sql benchmarking/sql_queries/entering_processed/activity_last_three_minutes.sql
COPY benchmarking/benchmark_config.yaml ./benchmarking/benchmark_config.yaml
COPY data ./data

RUN rm -rf /root/.cache

CMD ["tail", "-f", "/dev/null"]
