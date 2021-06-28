drop table if exists ods.tlc_green_trip;
create table if not exists ods.tlc_green_trip (
  `vendorid` bigint,
  `lpep_pickup_datetime` string,
  `lpep_dropoff_datetime` string,
  `store_and_fwd_flag` string,
  `ratecodeid` bigint,
  `pulocationid` bigint,
  `dolocationid` bigint,
  `passenger_count` bigint,
  `trip_distance` double,
  `fare_amount` double,
  `extra` double,
  `mta_tax` double,
  `tip_amount` double,
  `tolls_amount` double,
  `ehail_fee` string,
  `improvement_surcharge` double,
  `total_amount` double,
  `payment_type` bigint,
  `trip_type` bigint,
  `congestion_surcharge` double
)
partitioned by (year string, month string)
stored as parquet
location 's3://${data.bucket}/ods/tlc_green_trip/';