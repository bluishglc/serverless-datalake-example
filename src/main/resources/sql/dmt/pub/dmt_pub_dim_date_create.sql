-- noinspection sqlnodatasourceinspectionforfile
drop table if exists dmt.pub_dim_date;
create table if not exists dmt.pub_dim_date (
  dwid bigint,
  db_date string,
  year int,
  month int,
  day int,
  hour int,
  quarter int,
  week int,
  day_name string,
  month_name string,
  weekend_flag boolean
)
stored as parquet
location 's3://${data.bucket}/dmt/pub_dim_date/';