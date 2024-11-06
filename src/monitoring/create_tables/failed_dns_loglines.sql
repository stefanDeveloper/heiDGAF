CREATE TABLE IF NOT EXISTS server_logs (
    message_text String NOT NULL,
    timestamp_in DateTime64(6) NOT NULL,
    timestamp_failed DateTime64(6) NOT NULL,
    reason_for_failure String
)
ENGINE = MergeTree
PRIMARY KEY(message_id);
