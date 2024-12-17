CREATE TABLE IF NOT EXISTS logline_to_batches (
    logline_id UUID NOT NULL,
    batch_id UUID NOT NULL
)
ENGINE = MergeTree
PRIMARY KEY (logline_id);
