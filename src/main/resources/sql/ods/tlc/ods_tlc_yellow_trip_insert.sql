insert overwrite table ods.tlc_yellow_trip partition(year, month)
select
    *,
    '@year@',
    '@month@'
from
    v_stg_tlc_yellow_trip;