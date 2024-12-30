CREATE TABLE IF NOT EXISTS server_logs_timestamps (
    message_id UUID NOT NULL,
    event String NOT NULL,
    event_timestamp DateTime64(6) NOT NULL
)
ENGINE = MergeTree
PRIMARY KEY(message_id);
