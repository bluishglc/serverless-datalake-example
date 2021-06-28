-- noinspection SqlNoDataSourceInspectionForFile
insert into table dmt.tlc_union_trip
select
    'yellow'
    ,vendorid as vendorid
    ,tpep_pickup_datetime as pickup_datetime
    ,tpep_dropoff_datetime as dropoff_datetime
    ,passenger_count as passenger_count
    ,trip_distance as trip_distance
    ,ratecodeid as ratecodeid
    ,store_and_fwd_flag as store_and_fwd_flag
    ,pulocationid as pulocationid
    ,dolocationid as dolocationid
    ,payment_type as payment_type
    ,fare_amount as fare_amount
    ,extra as extra
    ,mta_tax as mta_tax
    ,tip_amount as tip_amount
    ,tolls_amount as tolls_amount
    ,improvement_surcharge as improvement_surcharge
    ,total_amount as total_amount
    ,congestion_surcharge as congestion_surcharge
    ,null as trip_type
    ,null as ehail_fee
    ,null as hvfhs_license_num
    ,null as dispatching_base_num
    ,null as sr_flag
    ,year as year
    ,month as month
from
    dwh.tlc_yellow_trip;

insert into table dmt.tlc_union_trip
select
    'green'
    ,vendorid as vendorid
    ,lpep_pickup_datetime as pickup_datetime
    ,lpep_dropoff_datetime as dropoff_datetime
    ,passenger_count as passenger_count
    ,trip_distance as trip_distance
    ,ratecodeid as ratecodeid
    ,store_and_fwd_flag as store_and_fwd_flag
    ,pulocationid as pulocationid
    ,dolocationid as dolocationid
    ,payment_type as payment_type
    ,fare_amount as fare_amount
    ,extra as extra
    ,mta_tax as mta_tax
    ,tip_amount as tip_amount
    ,tolls_amount as tolls_amount
    ,improvement_surcharge as improvement_surcharge
    ,total_amount as total_amount
    ,congestion_surcharge as congestion_surcharge
    ,trip_type as trip_type
    ,ehail_fee as ehail_fee
    ,null as hvfhs_license_num
    ,null as dispatching_base_num
    ,null as sr_flag
    ,year as year
    ,month as month
from
    dwh.tlc_green_trip;

insert into table dmt.tlc_union_trip
select
    'fhv'
    ,null as vendorid
    ,pickup_datetime as pickup_datetime
    ,dropoff_datetime as dropoff_datetime
    ,null as passenger_count
    ,null as trip_distance
    ,null as ratecodeid
    ,null as store_and_fwd_flag
    ,pulocationid as pulocationid
    ,dolocationid as dolocationid
    ,null as payment_type
    ,null as fare_amount
    ,null as extra
    ,null as mta_tax
    ,null as tip_amount
    ,null as tolls_amount
    ,null as improvement_surcharge
    ,null as total_amount
    ,null as congestion_surcharge
    ,null as trip_type
    ,null as ehail_fee
    ,null as hvfhs_license_num
    ,dispatching_base_num as dispatching_base_num
    ,sr_flag as sr_flag
    ,year as year
    ,month as month
from
    dwh.tlc_fhv_trip;

insert into table dmt.tlc_union_trip
select
    'fhvhv'
    ,null as vendorid
    ,pickup_datetime as pickup_datetime
    ,dropoff_datetime as dropoff_datetime
    ,null as passenger_count
    ,null as trip_distance
    ,null as ratecodeid
    ,null as store_and_fwd_flag
    ,pulocationid as pulocationid
    ,dolocationid as dolocationid
    ,null as payment_type
    ,null as fare_amount
    ,null as extra
    ,null as mta_tax
    ,null as tip_amount
    ,null as tolls_amount
    ,null as improvement_surcharge
    ,null as total_amount
    ,null as congestion_surcharge
    ,null as trip_type
    ,null as ehail_fee
    ,hvfhs_license_num as hvfhs_license_num
    ,dispatching_base_num as dispatching_base_num
    ,sr_flag as sr_flag
    ,year as year
    ,month as month
from
    dwh.tlc_fhvhv_trip;