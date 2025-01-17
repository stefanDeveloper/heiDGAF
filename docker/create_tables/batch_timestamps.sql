CREATE TABLE IF NOT EXISTS batch_timestamps (
    batch_id UUID NOT NULL,
    stage String NOT NULL,
    status String NOT NULL,
    timestamp DateTime64(6) NOT NULL,
    message_count UInt32,
    is_active Bool NOT NULL
)
ENGINE = MergeTree
PRIMARY KEY (batch_id);
