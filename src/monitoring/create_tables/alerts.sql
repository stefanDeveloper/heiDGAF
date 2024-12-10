CREATE TABLE IF NOT EXISTS server_logs_timestamps (
    client_ip String NOT NULL,
    alert_timestamp DateTime64(6) NOT NULL
)
ENGINE = MergeTree
PRIMARY KEY(client_ip, alert_timestamp);
