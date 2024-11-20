CREATE TABLE IF NOT EXISTS logline_status (
    logline_id UUID NOT NULL,
    status String NOT NULL,
    exit_at_stage Nullable(String)
)
ENGINE = MergeTree
PRIMARY KEY (logline_id);
