FROM python:3.9-slim

WORKDIR /project
COPY pyproject.toml ./
COPY heidgaf/ heidgaf/
RUN pip --disable-pip-version-check install --no-compile  .
RUN heidgaf --help

ENV TF_CPP_MIN_LOG_LEVEL=3
ENTRYPOINT [ "heidgaf"]
CMD [ "--help" ]