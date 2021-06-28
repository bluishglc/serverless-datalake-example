create or replace temporary view v_ods_geo_taxi_zone as
select
    *,
    unix_timestamp() as ts, -- append ts column for hudi
    '@year@' as year,
    '@month@' as month
from
    v_stg_geo_taxi_zone;