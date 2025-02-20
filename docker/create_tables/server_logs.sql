CREATE TABLE IF NOT EXISTS server_logs (
    message_id UUID NOT NULL,
    timestamp_in DateTime64(6) NOT NULL,
    message_text String NOT NULL
)
ENGINE = MergeTree
PRIMARY KEY(message_id);
