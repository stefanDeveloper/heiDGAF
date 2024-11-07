CREATE TABLE IF NOT EXISTS batch_timestamps (
    batch_id UUID NOT NULL,
    stage String NOT NULL,
    status String NOT NULL,
    timestamp DateTime64(6) NOT NULL,
    message_count UInt64
)
ENGINE = MergeTree
PRIMARY KEY (batch_id);
