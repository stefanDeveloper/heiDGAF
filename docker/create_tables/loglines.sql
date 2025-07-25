CREATE TABLE IF NOT EXISTS loglines (
    logline_id UUID NOT NULL,
    timestamp DateTime64(6) NOT NULL,
    client_ip String NOT NULL,
    additional_fields String
)
ENGINE = MergeTree
PRIMARY KEY (logline_id);
