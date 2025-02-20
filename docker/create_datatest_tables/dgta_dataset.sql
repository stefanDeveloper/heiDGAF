CREATE TABLE IF NOT EXISTS dgta_dataset (
    query String,
    class Int32,
    labels Array(String),
    tld String,
    fqdn String,
    secondleveldomain String,
    thirdleveldomain String
)
ENGINE = MergeTree
PRIMARY KEY(query);
