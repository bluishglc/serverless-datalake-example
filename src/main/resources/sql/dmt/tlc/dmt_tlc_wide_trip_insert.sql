-- noinspection SqlNoDataSourceInspectionForFile
insert overwrite table dmt.tlc_wide_trip
select
    row_number() over(order by 0) as dwid,
    t.tlc_trip_type,
    t.vendorid,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.ratecodeid,
    t.store_and_fwd_flag,
    t.pulocationid,
    t.dolocationid,
    t.payment_type,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    t.congestion_surcharge,
    t.trip_type,
    t.ehail_fee,
    t.hvfhs_license_num,
    t.dispatching_base_num,
    t.sr_flag,
    case
        when t.trip_distance is not null and t.trip_distance <= 1.0 then '0-1 Mile'
        when t.trip_distance is not null and t.trip_distance <= 2.0 then '1-2 Miles'
        when t.trip_distance is not null and t.trip_distance <= 3.0 then '2-3 Miles'
        when t.trip_distance is not null and t.trip_distance <= 4.0 then '3-4 Miles'
        when t.trip_distance is not null and t.trip_distance <= 5.0 then '4-5 Miles'
        when t.trip_distance is not null and t.trip_distance <= 6.0 then '5-6 Miles'
        when t.trip_distance is not null and t.trip_distance <= 7.0 then '6-7 Miles'
        when t.trip_distance is not null and t.trip_distance <= 8.0 then '7-8 Miles'
        when t.trip_distance is not null and t.trip_distance <= 9.0 then '8-9 Miles'
        when t.trip_distance is not null and t.trip_distance <= 10.0 then '9-10 Miles'
        when t.trip_distance is not null and t.trip_distance <= 15.0 then '10-15 Miles'
        when t.trip_distance is not null and t.trip_distance <= 20.0 then '15-20 Miles'
        when t.trip_distance is not null and t.trip_distance <= 25.0 then '20-25 Miles'
        when t.trip_distance is not null and t.trip_distance <= 30.0 then '25-30 Miles'
        when t.trip_distance is not null and t.trip_distance > 30.0 then '30 Miles Above'
        else null
    end as travel_distance_rank,
    (unix_timestamp(t.dropoff_datetime) - unix_timestamp(t.pickup_datetime))/60 as travel_time,
    case
        when (unix_timestamp(t.dropoff_datetime) - unix_timestamp(t.pickup_datetime))/60 <= 10.0 then '0-10 Minutes'
        when (unix_timestamp(t.dropoff_datetime) - unix_timestamp(t.pickup_datetime))/60 <= 20.0 then '10-20 Minutes'
        when (unix_timestamp(t.dropoff_datetime) - unix_timestamp(t.pickup_datetime))/60 <= 30.0 then '20-30 Minutes'
        when (unix_timestamp(t.dropoff_datetime) - unix_timestamp(t.pickup_datetime))/60 <= 40.0 then '30-40 Minutes'
        when (unix_timestamp(t.dropoff_datetime) - unix_timestamp(t.pickup_datetime))/60 <= 50.0 then '40-50 Minutes'
        when (unix_timestamp(t.dropoff_datetime) - unix_timestamp(t.pickup_datetime))/60 <= 60.0 then '50-60 Minutes'
        when (unix_timestamp(t.dropoff_datetime) - unix_timestamp(t.pickup_datetime))/60 > 60.0 then '60 Minutes Above'
        else null
    end as travel_time_rank,
    case
        when t.trip_distance is not null then t.trip_distance/((unix_timestamp(t.dropoff_datetime) - unix_timestamp(t.pickup_datetime))/3600)
        else null
    end as travel_average_speed,
    case
        when t.trip_distance is not null and t.trip_distance/((unix_timestamp(t.dropoff_datetime) - unix_timestamp(t.pickup_datetime))/3600) <= 5.0 then '0-5 Miles/Hour'
        when t.trip_distance is not null and t.trip_distance/((unix_timestamp(t.dropoff_datetime) - unix_timestamp(t.pickup_datetime))/3600) <= 10.0 then '5-10 Miles/Hour'
        when t.trip_distance is not null and t.trip_distance/((unix_timestamp(t.dropoff_datetime) - unix_timestamp(t.pickup_datetime))/3600) <= 15.0 then '10-15 Miles/Hour'
        when t.trip_distance is not null and t.trip_distance/((unix_timestamp(t.dropoff_datetime) - unix_timestamp(t.pickup_datetime))/3600) <= 20.0 then '15-20 Miles/Hour'
        when t.trip_distance is not null and t.trip_distance/((unix_timestamp(t.dropoff_datetime) - unix_timestamp(t.pickup_datetime))/3600) <= 25.0 then '20-25 Miles/Hour'
        when t.trip_distance is not null and t.trip_distance/((unix_timestamp(t.dropoff_datetime) - unix_timestamp(t.pickup_datetime))/3600) <= 30.0 then '25-30 Miles/Hour'
        when t.trip_distance is not null and t.trip_distance/((unix_timestamp(t.dropoff_datetime) - unix_timestamp(t.pickup_datetime))/3600) > 30.0 then '30 Miles/Hour Above'
        else null
    end as travel_average_speed_rank,
    h.dwid as hour_dwid,
    h.db_date,
    h.day,
    h.hour,
    h.quarter,
    h.week,
    h.day_name,
    h.month_name,
    h.weekend_flag,
    puz.borough as pu_borough,
    puz.zone as pu_zone,
    puz.service_zone as pu_service_zone,
    doz.borough as do_borough,
    doz.zone as do_zone,
    doz.service_zone as do_service_zone,
    concat(puz.zone, ' -> ', doz.zone) as pu_do_zone,
    case t.tlc_trip_type in ('yellow','green')
        when true then t.total_amount/((unix_timestamp(t.dropoff_datetime) - unix_timestamp(t.pickup_datetime))/60)
        else null
    end as time_profit_rate,
    case t.tlc_trip_type in ('yellow','green')
        when true then t.total_amount/t.trip_distance
        else null
    end as distance_profit_rate,
    t.year,
    t.month
from
    dmt.tlc_union_trip t
join
    dmt.pub_dim_date h
on cast(date_format(cast(t.pickup_datetime as timestamp), 'yyyyMMddHH') as bigint) = h.dwid
join
    dmt.geo_dim_taxi_zone puz
on
    t.pulocationid = puz.locationid
join
    dmt.geo_dim_taxi_zone doz
on
    t.dolocationid = doz.locationid;