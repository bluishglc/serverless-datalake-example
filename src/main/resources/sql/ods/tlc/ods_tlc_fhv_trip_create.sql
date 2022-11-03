drop table if exists ods.tlc_fhv_trip;
create table if not exists ods.tlc_fhv_trip(
  `dispatching_base_num` string,
  `pickup_datetime` string,
  `dropoff_datetime` string,
  `pulocationid` bigint,
  `dolocationid` bigint,
  `sr_flag` string
)
partitioned by (year string, month string)
stored as parquet
location 's3://${data.bucket}/ods/tlc_fhv_trip/';