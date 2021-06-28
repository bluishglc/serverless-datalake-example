insert overwrite table ods.tlc_green_trip partition(year, month)
select
    *,
    '@year@',
    '@month@'
from 
    v_stg_tlc_green_trip;