insert overwrite table ods.tlc_fhv_trip partition(year, month)
select
    *,
    '@year@',
    '@month@'
from 
    v_stg_tlc_fhv_trip;