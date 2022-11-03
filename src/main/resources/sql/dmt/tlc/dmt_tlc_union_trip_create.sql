-- noinspection sqlnodatasourceinspectionforfile

drop table if exists dmt.tlc_union_trip;
create table if not exists dmt.tlc_union_trip (
   tlc_trip_type 	     string
  ,vendorid              bigint
  ,pickup_datetime       string
  ,dropoff_datetime      string
  ,passenger_count       bigint
  ,trip_distance         double
  ,ratecodeid            bigint
  ,store_and_fwd_flag    bigint
  ,pulocationid          bigint
  ,dolocationid          bigint
  ,payment_type          bigint
  ,fare_amount           double
  ,extra                 double
  ,mta_tax               double
  ,tip_amount            double
  ,tolls_amount          double
  ,improvement_surcharge double
  ,total_amount          double
  ,congestion_surcharge  double
  ,trip_type             bigint
  ,ehail_fee             double
  ,hvfhs_license_num     string
  ,dispatching_base_num  string
  ,sr_flag               string
)
partitioned by (year string, month string)
stored as parquet
location 's3://${data.bucket}/dmt/tlc_union_trip/';