set hoodie.dwh.geo_taxi_zone.consume.mode=INCREMENTAL;
set hoodie.dwh.geo_taxi_zone.consume.max.commits=1;
set hoodie.dwh.geo_taxi_zone.consume.start.timestamp=@from@;

create or replace temporary view v_dmt_geo_dim_taxi_zone as
select
    locationid,
    borough,
    zone,
    ts, -- append ts column for hudi
    replace(service_zone,'/','') as service_zone, -- remove anti-slash, otherwise it can't be partition column!
    year,
    month
from
    dwh.geo_taxi_zone;