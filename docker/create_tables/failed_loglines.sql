CREATE TABLE IF NOT EXISTS failed_loglines (
    message_text String NOT NULL,
    timestamp_in DateTime64(6) NOT NULL,
    timestamp_failed DateTime64(6) NOT NULL,
    reason_for_failure Nullable(String)
)
ENGINE = MergeTree
PRIMARY KEY(message_text, timestamp_in);
