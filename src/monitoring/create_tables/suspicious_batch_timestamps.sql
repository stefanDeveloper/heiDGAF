CREATE TABLE IF NOT EXISTS suspicious_batch_timestamps (
    suspicious_batch_id UUID NOT NULL,
    client_ip String NOT NULL,
    stage String NOT NULL,
    status String NOT NULL,
    timestamp DateTime64(6) NOT NULL,
    message_count UInt32,
    is_active Bool NOT NULL
)
ENGINE = MergeTree
PRIMARY KEY (suspicious_batch_id);
