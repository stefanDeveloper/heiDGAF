FROM python:3

WORKDIR /usr/src/app

COPY requirements.lc.txt ./
RUN pip --disable-pip-version-check install --no-cache-dir --no-compile  -r requirements.lc.txt

COPY heidgaf_core ./heidgaf_core
COPY heidgaf_log_collection ./heidgaf_log_collection
COPY kafka_config.yaml .

CMD [ "python", "heidgaf_log_collection/server.py"]
