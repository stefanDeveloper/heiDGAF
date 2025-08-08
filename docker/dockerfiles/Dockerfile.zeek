FROM zeek/zeek:latest

RUN apt update -y && apt upgrade -y
RUN apt install -y \
    build-essential \
    cmake \
    librdkafka-dev \
    libssl-dev \
    libpcap-dev \
    vim \
    iproute2 \
    python3-pip

# install the zeek kafka plugin
RUN yes | zkg install zeek-kafka --user-var LIBRDKAFKA_ROOT=/usr/local
RUN setcap cap_net_raw,cap_net_admin=+eip $(which zeek)

RUN rm /usr/lib/python3.11/EXTERNALLY-MANAGED
COPY requirements/requirements.zeek.txt /opt/requirements.txt
RUN pip3 install -r /opt/requirements.txt

RUN chown -R root:root /usr/local/zeek
RUN mkdir /opt/logs
WORKDIR /opt/logs

RUN mkdir "/opt/static_files"
ENV STATIC_FILES_DIR="/opt/static_files"

CMD ["bash", "-c", "cd /opt/ && python3 /opt/src/zeek/zeek_handler.py -c /opt/config.yaml"]
