CREATE TABLE IF NOT EXISTS fill_levels (
    timestamp DateTime64(6) NOT NULL,
    stage String NOT NULL,
    entry_type String NOT NULL,
    entry_count UInt32 DEFAULT 0
)
ENGINE = MergeTree
PRIMARY KEY (timestamp, stage, entry_type);
