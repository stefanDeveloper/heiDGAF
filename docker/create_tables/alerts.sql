CREATE TABLE IF NOT EXISTS alerts (
    src_ip String NOT NULL,
    alert_timestamp DateTime64(6) NOT NULL,
    suspicious_batch_id UUID NOT NULL,
    overall_score Float32 NOT NULL,
    domain_names String NOT NULL,
    result String,
)
ENGINE = MergeTree
PRIMARY KEY(src_ip, alert_timestamp);
