insert overwrite table dmt.pub_dim_date
select
    *
from
    v_stg_pub_dim_date;