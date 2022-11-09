from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

import datahub.emitter.mce_builder as builder
from datahub_provider.operators.datahub import DatahubEmitterOperator

default_args = {
    'owner': 'sdl',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'end_date': datetime(2020, 1, 31),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'sdl_monthly_build',
    default_args=default_args,
    description='monthly build for serverless datalake',
    schedule_interval='@monthly',
)

# stg_pub

stg_pub_data_feed_cmd = """
sdl-feeder feed-pub-data
"""

stg_pub_data_feed = SSHOperator(
    ssh_conn_id='ssh_to_client',
    task_id='stg_pub_data_feed',
    command=stg_pub_data_feed_cmd,
    dag=dag
)

# stg_geo

stg_geo_data_feed_cmd = """
sdl-feeder feed-geo-data \
    --year {{execution_date.year}} \
    --month {{execution_date.month}}
"""

stg_geo_data_feed = SSHOperator(
    ssh_conn_id='ssh_to_client',
    task_id='stg_geo_data_feed',
    command=stg_geo_data_feed_cmd,
    dag=dag
)

# stg_tlc

stg_tlc_data_feed_cmd = """
sdl-feeder feed-tlc-data \
    --year {{execution_date.year}} \
    --month {{execution_date.month}}
"""

stg_tlc_data_feed = SSHOperator(
    ssh_conn_id='ssh_to_client',
    task_id='stg_tlc_data_feed',
    command=stg_tlc_data_feed_cmd,
    dag=dag
)

# ods_geo

ods_geo_taxi_zone_insert_cmd = """
sdl-job start \
    --job-name ods_geo_taxi_zone_insert \
    --year {{execution_date.year}} \
    --month {{execution_date.month}}
"""

ods_geo_taxi_zone_insert = SSHOperator(
    ssh_conn_id='ssh_to_client',
    task_id='ods_geo_taxi_zone_insert',
    command=ods_geo_taxi_zone_insert_cmd,
    dag=dag
)

ods_geo_taxi_zone_lineage = DatahubEmitterOperator(
    task_id="ods_geo_taxi_zone_lineage",
    datahub_conn_id="datahub_kafka_default",
    mces=[
        builder.make_lineage_mce(
            upstream_urns=[
                builder.make_dataset_urn("glue", "stg.geo_taxi_zone")
            ],
            downstream_urn=builder.make_dataset_urn("glue", "ods.geo_taxi_zone"),
        )
    ],
)

# ods_tlc

ods_tlc_yellow_trip_insert_cmd = """
sdl-job start \
    --job-name ods_tlc_yellow_trip_insert \
    --year {{execution_date.year}} \
    --month {{execution_date.month}}
"""

ods_tlc_yellow_trip_insert = SSHOperator(
    ssh_conn_id='ssh_to_client',
    task_id='ods_tlc_yellow_trip_insert',
    command=ods_tlc_yellow_trip_insert_cmd,
    dag=dag
)

ods_tlc_green_trip_insert_cmd = """
sdl-job start \
    --job-name ods_tlc_green_trip_insert \
    --year {{execution_date.year}} \
    --month {{execution_date.month}}
"""

ods_tlc_green_trip_insert = SSHOperator(
    ssh_conn_id='ssh_to_client',
    task_id='ods_tlc_green_trip_insert',
    command=ods_tlc_green_trip_insert_cmd,
    dag=dag
)

ods_tlc_fhv_trip_insert_cmd = """
sdl-job start \
    --job-name ods_tlc_fhv_trip_insert \
    --year {{execution_date.year}} \
    --month {{execution_date.month}}
"""

ods_tlc_fhv_trip_insert = SSHOperator(
    ssh_conn_id='ssh_to_client',
    task_id='ods_tlc_fhv_trip_insert',
    command=ods_tlc_fhv_trip_insert_cmd,
    dag=dag
)

ods_tlc_fhvhv_trip_insert_cmd = """
sdl-job start \
    --job-name ods_tlc_fhvhv_trip_insert \
    --year {{execution_date.year}} \
    --month {{execution_date.month}}
"""

ods_tlc_fhvhv_trip_insert = SSHOperator(
    ssh_conn_id='ssh_to_client',
    task_id='ods_tlc_fhvhv_trip_insert',
    command=ods_tlc_fhvhv_trip_insert_cmd,
    dag=dag
)

# dwh_geo

dwh_geo_taxi_zone_upsert_cmd = """
sdl-job start \
    --job-name dwh_geo_taxi_zone_upsert \
    --year {{execution_date.year}} \
    --month {{execution_date.month}}
"""

dwh_geo_taxi_zone_upsert = SSHOperator(
    ssh_conn_id='ssh_to_client',
    task_id='dwh_geo_taxi_zone_upsert',
    command=dwh_geo_taxi_zone_upsert_cmd,
    dag=dag
)

# dwh_tlc

dwh_tlc_yellow_trip_insert_cmd = """
sdl-job start \
    --job-name dwh_tlc_yellow_trip_insert \
    --year {{execution_date.year}} \
    --month {{execution_date.month}}
"""

dwh_tlc_yellow_trip_insert = SSHOperator(
    ssh_conn_id='ssh_to_client',
    task_id='dwh_tlc_yellow_trip_insert',
    command=dwh_tlc_yellow_trip_insert_cmd,
    dag=dag
)

dwh_tlc_green_trip_insert_cmd = """
sdl-job start \
    --job-name dwh_tlc_green_trip_insert \
    --year {{execution_date.year}} \
    --month {{execution_date.month}}
"""

dwh_tlc_green_trip_insert = SSHOperator(
    ssh_conn_id='ssh_to_client',
    task_id='dwh_tlc_green_trip_insert',
    command=dwh_tlc_green_trip_insert_cmd,
    dag=dag
)

dwh_tlc_fhv_trip_insert_cmd = """
sdl-job start \
    --job-name dwh_tlc_fhv_trip_insert \
    --year {{execution_date.year}} \
    --month {{execution_date.month}}
"""

dwh_tlc_fhv_trip_insert = SSHOperator(
    ssh_conn_id='ssh_to_client',
    task_id='dwh_tlc_fhv_trip_insert',
    command=dwh_tlc_fhv_trip_insert_cmd,
    dag=dag
)

dwh_tlc_fhvhv_trip_insert_cmd = """
sdl-job start \
    --job-name dwh_tlc_fhvhv_trip_insert \
    --year {{execution_date.year}} \
    --month {{execution_date.month}}
"""

dwh_tlc_fhvhv_trip_insert = SSHOperator(
    ssh_conn_id='ssh_to_client',
    task_id='dwh_tlc_fhvhv_trip_insert',
    command=dwh_tlc_fhvhv_trip_insert_cmd,
    dag=dag
)

# dmt_pub

dmt_pub_dim_date_overwrite_cmd = """
sdl-job start \
    --job-name dmt_pub_dim_date_overwrite \
    --year {{execution_date.year}} \
    --month {{execution_date.month}}
"""

dmt_pub_dim_date_overwrite = SSHOperator(
    ssh_conn_id='ssh_to_client',
    task_id='dmt_pub_dim_date_overwrite',
    command=dmt_pub_dim_date_overwrite_cmd,
    dag=dag
)

# dmt_geo

dmt_geo_dim_taxi_zone_upsert_cmd = """
sdl-job start \
    --job-name dmt_geo_dim_taxi_zone_upsert \
    --year {{execution_date.year}} \
    --month {{execution_date.month}}
"""

dmt_geo_dim_taxi_zone_upsert = SSHOperator(
    ssh_conn_id='ssh_to_client',
    task_id='dmt_geo_dim_taxi_zone_upsert',
    command=dmt_geo_dim_taxi_zone_upsert_cmd,
    dag=dag
)

# dmt_tlc

dmt_tlc_union_trip_insert_cmd = """
sdl-job start \
    --job-name dmt_tlc_union_trip_insert \
    --year {{execution_date.year}} \
    --month {{execution_date.month}}
"""

dmt_tlc_union_trip_insert = SSHOperator(
    ssh_conn_id='ssh_to_client',
    task_id='dmt_tlc_union_trip_insert',
    command=dmt_tlc_union_trip_insert_cmd,
    dag=dag
)

dmt_tlc_wide_trip_insert_cmd = """
sdl-job start \
    --job-name dmt_tlc_wide_trip_insert \
    --year {{execution_date.year}} \
    --month {{execution_date.month}}
"""

dmt_tlc_wide_trip_insert = SSHOperator(
    ssh_conn_id='ssh_to_client',
    task_id='dmt_tlc_wide_trip_insert',
    command=dmt_tlc_wide_trip_insert_cmd,
    dag=dag
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

stg_pub_to_dmt_pub = DummyOperator(task_id='stg_pub_to_dmt_pub', dag=dag)

stg_geo_to_ods_geo = DummyOperator(task_id='stg_geo_to_ods_geo', dag=dag)
ods_geo_to_dwh_geo = DummyOperator(task_id='ods_geo_to_dwh_geo', dag=dag)
dwh_geo_to_dmt_geo = DummyOperator(task_id='dwh_geo_to_dmt_geo', dag=dag)

stg_tlc_to_ods_tlc = DummyOperator(task_id='stg_tlc_to_ods_tlc', dag=dag)
ods_tlc_to_dwh_tlc = DummyOperator(task_id='ods_tlc_to_dwh_tlc', dag=dag)
dwh_tlc_to_dmt_tlc = DummyOperator(task_id='dwh_tlc_to_dmt_tlc', dag=dag)

start >> stg_pub_data_feed >> stg_pub_to_dmt_pub >> dmt_pub_dim_date_overwrite >> dmt_tlc_wide_trip_insert >> end
start >> stg_geo_data_feed >> stg_geo_to_ods_geo >> ods_geo_taxi_zone_insert >> ods_geo_to_dwh_geo >> dwh_geo_taxi_zone_upsert >> dwh_geo_to_dmt_geo >> dmt_geo_dim_taxi_zone_upsert >> dmt_tlc_wide_trip_insert >> end
start >> stg_tlc_data_feed >> stg_tlc_to_ods_tlc >> [ods_tlc_yellow_trip_insert, ods_tlc_green_trip_insert, ods_tlc_fhv_trip_insert, ods_tlc_fhvhv_trip_insert] >> ods_tlc_to_dwh_tlc >> [dwh_tlc_yellow_trip_insert, dwh_tlc_green_trip_insert, dwh_tlc_fhv_trip_insert, dwh_tlc_fhvhv_trip_insert] >> dwh_tlc_to_dmt_tlc >> dmt_tlc_union_trip_insert >> dmt_tlc_wide_trip_insert >> end


