drop table if exists ods.tlc_fhvhv_trip;
create table if not exists ods.tlc_fhvhv_trip (
  `hvfhs_license_num` string,
  `dispatching_base_num` string,
  `pickup_datetime` string,
  `dropoff_datetime` string,
  `pulocationid` bigint,
  `dolocationid` bigint,
  `sr_flag` bigint
)
partitioned by (year string, month string)
stored as parquet
location 's3://${data.bucket}/ods/tlc_fhvhv_trip/';