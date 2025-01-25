CREATE TABLE geoip(
    ip_range_start IPv4,
    ip_range_end IPv4,
    country_code Nullable(String),
    state1 Nullable(String),
    state2 Nullable(String),
    city Nullable(String),
    postcode Nullable(String),
    latitude Float64,
    longitude Float64,
    timezone Nullable(String)
) engine=URL('https://raw.githubusercontent.com/sapics/ip-location-db/master/dbip-city/dbip-city-ipv4.csv.gz', 'CSV');
