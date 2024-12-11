CREATE TABLE IF NOT EXISTS alerts (
    client_ip String NOT NULL,
    alert_timestamp DateTime64(6) NOT NULL,
    suspicious_batch_id UUID NOT NULL
)
ENGINE = MergeTree
PRIMARY KEY(client_ip, alert_timestamp);
