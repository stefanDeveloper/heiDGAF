CREATE TABLE IF NOT EXISTS batch_status (
    batch_id UUID NOT NULL,
    status String NOT NULL,
    exit_at_stage Nullable(String)
)
ENGINE = MergeTree
PRIMARY KEY (batch_id);
