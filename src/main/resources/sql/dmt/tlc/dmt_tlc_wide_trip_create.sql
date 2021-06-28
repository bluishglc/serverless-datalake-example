-- noinspection sqlnodatasourceinspectionforfile

drop table if exists dmt.tlc_wide_trip;
create table if not exists dmt.tlc_wide_trip (
   dwid                  bigint
  ,tlc_trip_type 	     string
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
  ,travel_distance_rank  string
  ,travel_time double
  ,travel_time_rank      string
  ,travel_average_speed double
  ,travel_average_speed_rank string
  ,hour_dwid bigint
  ,db_date string
  ,day int
  ,hour int
  ,quarter int
  ,week int
  ,day_name string
  ,month_name string
  ,weekend_flag boolean
  ,pu_borough string
  ,pu_zone string
  ,pu_service_zone string
  ,do_borough string
  ,do_zone string
  ,do_service_zone string
  ,pu_do_zone string
  ,time_profit_rate double
  ,distance_profit_rate double
)
partitioned by (year string, month string)
stored as parquet
location 's3://${data.bucket}/dmt/tlc_wide_trip/';
