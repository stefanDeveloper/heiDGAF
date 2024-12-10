CREATE TABLE IF NOT EXISTS logline_timestamps (
    logline_id UUID NOT NULL,
    stage String NOT NULL,
    status String NOT NULL,
    timestamp DateTime64(6) NOT NULL,
    is_active Bool NOT NULL
)
ENGINE = MergeTree
PRIMARY KEY (logline_id);
