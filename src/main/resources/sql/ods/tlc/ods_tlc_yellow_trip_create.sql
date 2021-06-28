drop table if exists ods.tlc_yellow_trip;
create table if not exists ods.tlc_yellow_trip (
    `vendorid` bigint,
    `tpep_pickup_datetime` string,
    `tpep_dropoff_datetime` string,
    `passenger_count` bigint,
    `trip_distance` double,
    `ratecodeid` bigint,
    `store_and_fwd_flag` string,
    `pulocationid` bigint,
    `dolocationid` bigint,
    `payment_type` bigint,
    `fare_amount` double,
    `extra` double,
    `mta_tax` double,
    `tip_amount` double,
    `tolls_amount` double,
    `improvement_surcharge` double,
    `total_amount` double,
    `congestion_surcharge` double
)
partitioned by (year string, month string)
stored as parquet
location 's3://${data.bucket}/ods/tlc_yellow_trip/';