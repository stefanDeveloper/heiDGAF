FROM python:3.10-bookworm

WORKDIR /project

COPY requirements.txt ./
COPY pyproject.toml ./
COPY heidgaf/ heidgaf/

RUN pip --disable-pip-version-check install --no-cache-dir --no-compile  .

CMD [ "heidgaf", "train" ]