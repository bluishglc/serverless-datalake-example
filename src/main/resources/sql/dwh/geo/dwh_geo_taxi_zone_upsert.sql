set hoodie.ods.geo_taxi_zone.consume.mode=INCREMENTAL;
set hoodie.ods.geo_taxi_zone.consume.max.commits=1;
set hoodie.ods.geo_taxi_zone.consume.start.timestamp=@from@;

create or replace temporary view v_dwh_geo_taxi_zone as
select
    `(_hoodie_commit_time|_hoodie_commit_seqno|_hoodie_record_key|hoodie_partition_path|_hoodie_file_name)?+.+`
from
    ods.geo_taxi_zone;