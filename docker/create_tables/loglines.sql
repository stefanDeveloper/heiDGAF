CREATE TABLE IF NOT EXISTS loglines (
    logline_id UUID NOT NULL,
    timestamp DateTime64(6) NOT NULL,
    subnet_id String NOT NULL,
    src_ip String NOT NULL,
    additional_fields String
)
ENGINE = MergeTree
PRIMARY KEY (logline_id);
