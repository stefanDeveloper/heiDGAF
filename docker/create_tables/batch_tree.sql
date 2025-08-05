-- Table to be able to reconstruct where the batch was processed in
-- used in grafana to calculate the elapsed time between stages
CREATE TABLE IF NOT EXISTS batch_tree (
    batch_row_id String NOT NULL,
    batch_id UUID NOT NULL,
    parent_batch_row_id Nullable(String), -- Default of Null indicates a root element
    instance_name String NOT NULL,
    stage String NOT NULL,
    status String NOT NULL,
    timestamp DateTime64(6) NOT NULL,
)
ENGINE = MergeTree
-- keep the PK as the UUID even thogh it is not uinque for indexing reasons
PRIMARY KEY (batch_row_id);
