#!/usr/bin/env bash

REGION="${region}"
ROLE_ARN="${role.arn}"
APP_BUCKET="${app.bucket}"
DATA_BUCKET="${data.bucket}"
AIRFLOW_DAGS_HOME="${airflow.dags.home}"

NYC_TLC_ACCESS_KEY_ID="${nyc.tlc.access.key.id}"
NYC_TLC_SECRET_ACCESS_KEY="${nyc.tlc.secret.access.key}"

ADHOC_SQL_JOB="@sql"
GLUE_MAX_CAPACITY="${glue.max-capacity}"
GLUE_MAX_CONCURRENT_RUNS_PER_JOB="${glue.max-concurrent-runs-per-job}"