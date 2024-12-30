CREATE TABLE IF NOT EXISTS suspicious_batches_to_batch (
    suspicious_batch_id UUID NOT NULL,
    batch_id UUID NOT NULL
)
ENGINE = MergeTree
PRIMARY KEY (suspicious_batch_id);
