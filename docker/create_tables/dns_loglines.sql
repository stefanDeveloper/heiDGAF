CREATE TABLE IF NOT EXISTS dns_loglines (
    logline_id UUID NOT NULL,
    subnet_id String NOT NULL,
    timestamp DateTime64(6) NOT NULL,
    status_code String NOT NULL,
    client_ip String NOT NULL,
    record_type String NOT NULL,
    additional_fields String
)
ENGINE = MergeTree
PRIMARY KEY (logline_id);
