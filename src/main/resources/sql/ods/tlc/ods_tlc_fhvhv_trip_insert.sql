insert overwrite table ods.tlc_fhvhv_trip partition(year, month)
select
    *,
    '@year@',
    '@month@'
from 
    v_stg_tlc_fhvhv_trip;