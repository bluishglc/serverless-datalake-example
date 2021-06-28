#!/usr/bin/env bash

SDL_HOME="$(cd "`dirname $(readlink -nf "$0")`"/..; pwd -P)"

source "$SDL_HOME/bin/constants.sh"
source "$SDL_HOME/bin/util.sh"

JOB_NAME=""
JOB_ALIAS=""
JOB_TAGS="app=sdl"
SRC_ACTION=""
SRC_TABLE=""
ACTION=""
SQL=""
SQL_FILES=""
SQL_PARAMS=""
SINK_ACTION=""
SINK_TABLE=""
HUDI_RECORD_KEY_FIELD=""
HUDI_PRECOMBINE_FIELD=""
HUDI_PARTITION_PATH_FIELD=""

JOB_CLASS="com.github.sdl.GenericJob"
JOB_ARGS_DIR="$SDL_HOME/log/job-args"
ADHOC_SQL_DIR="$SDL_HOME/log/adhoc-sqls"

# ----------------------------------------------    Script Functions    ---------------------------------------------- #

createJob() {
    # delete job first if exists
    deleteJob
    printHeading "CREATING JOB [ $JOB_NAME ]"
    # make job args file...
    mkdir -p "$JOB_ARGS_DIR"
    jobFile="$JOB_ARGS_DIR/${JOB_NAME}.json"
    cat <<-EOF > "$jobFile"
    {
        "Name": "$JOB_NAME",
        "Role": "$ROLE_ARN",
        "ExecutionProperty": {
            "MaxConcurrentRuns": $GLUE_MAX_CONCURRENT_RUNS_PER_JOB
        },
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": "s3://$APP_BUCKET/src/$(tr '.' '/' <<< "$JOB_CLASS").scala",
            "PythonVersion": "3"
        },
        "DefaultArguments": {
            "--job-language": "scala",
            "--class": "$JOB_CLASS",
            "--extra-jars": "$(resolveDependentJars)",
            "--extra-files": "$(resolveConfigFiles)",
            "--enable-spark-ui": "true",
            "--spark-event-logs-path": "s3://$APP_BUCKET/log/spark-event-logs",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-glue-datacatalog": "",
            "--enable-metrics": "",
            "--enable-s3-parquet-optimized-committer": "",
            "--src-action": "",
            "--src-table": "",
            "--action": "",
            "--sql-files": "",
            "--sql-params": "",
            "--sink-action": "",
            "--sink-table": "",
            "--hudi-record-key-field": "",
            "--hudi-precombine-field": "",
            "--hudi-partition-path-field": ""
        },
        "MaxRetries": 0,
        "MaxCapacity": $GLUE_MAX_CAPACITY,
        "GlueVersion": "2.0"
    }
EOF
    echo "Creating Job Output:"
    echo ""
    aws glue create-job --region "$REGION" --tags "${JOB_TAGS:-""}" --cli-input-json file://$jobFile
    echo -ne "\nThe job [ $JOB_NAME ] is [ CREATED ]\n\n"
}

deleteJob() {
    printHeading "DELETE JOB [ $JOB_NAME ]"
    echo "Deleting Job Output:"
    echo ""
    aws glue delete-job --region $REGION --job-name $JOB_NAME
    echo ""
    echo "The job [ $JOB_NAME ] is [ DELETED ]"
}

startJob() {
    # If $JOB_ALIAS set, append it to JOB_NAME
    preferredJobName=$([ ! "$JOB_ALIAS" == "" ] && echo $JOB_ALIAS || echo $JOB_NAME)
    printHeading "START JOB [ $preferredJobName ]"

    # make job args file...
    DATE="$(date +%F)"
    NANO_SEC="$(date +%s%N)"
    mkdir -p "$JOB_ARGS_DIR/$DATE"
    jobRunFile="$JOB_ARGS_DIR/$DATE/${JOB_NAME}_${NANO_SEC}.json"
    cat <<-EOF > "$jobRunFile"
    {
        "JobName": "$JOB_NAME",
        "Arguments": {
            "--src-action": "$SRC_ACTION",
            "--src-table": "$SRC_TABLE",
            "--action": "$ACTION",
            "--sql-files": "$SQL_FILES",
            "--sql-params": "$SQL_PARAMS",
            "--sink-action": "$SINK_ACTION",
            "--sink-table": "$SINK_TABLE",
            "--hudi-record-key-field": "$HUDI_RECORD_KEY_FIELD",
            "--hudi-precombine-field": "$HUDI_PRECOMBINE_FIELD",
            "--hudi-partition-path-field": "$HUDI_PARTITION_PATH_FIELD"
        },
        "MaxCapacity": $GLUE_MAX_CAPACITY
    }
EOF
    echo "Job Run Configs:"
    echo ""
    cat "$jobRunFile"
    echo ""
    jobRunId="$(aws glue start-job-run --region "$REGION" --cli-input-json file://$jobRunFile --query "JobRunId" --output text)"
    # rename job args file with job id as log
    mv "$jobRunFile" "$JOB_ARGS_DIR/$(date +%F)/${JOB_NAME}_${jobRunId}.json"
    now=$(date +%s)sec
    while true; do
        jobStatus="$(aws glue get-job-run --region "$REGION" --job-name "$JOB_NAME" --run-id "$jobRunId" --query "JobRun.JobRunState" --output text)"
        if [ "$jobStatus" == "STARTING" ] || [ "$jobStatus" == "RUNNING" ] || [ "$jobStatus" == "STOPPING" ]; then
            for i in {0..5}; do
                echo -ne "\E[33;5m>>> The job [ $jobRunId ] state is [ $jobStatus ], duration [ $(TZ=UTC date --date now-$now +%H:%M:%S) ] ....\r\E[0m"
                sleep 1
            done
        else
            echo -ne "\e[0A\e[KThe job [ $preferredJobName ] is [ $jobStatus ]\n\n"
            if [ "$jobStatus" == "FAILED" ] || [ "$jobStatus" == "ERROR" ] || [ "$jobStatus" == "TIMEOUT" ]; then
                exit 1
            else
                break
            fi
        fi
    done
}

# execSqlFiles actually is starting a sql-specific job
execSqlFiles() {
    JOB_NAME="$ADHOC_SQL_JOB"
    ACTION="sql"
    startJob
}

execSql() {
    DATE="$(date +%F)"
    NANO_SEC="$(date +%s.%N)"
    mkdir -p "$ADHOC_SQL_DIR/$DATE"
    echo "$SQL" > "$ADHOC_SQL_DIR/$DATE/$NANO_SEC.sql"
    aws s3 cp "$ADHOC_SQL_DIR/$DATE/$NANO_SEC.sql" "s3://$APP_BUCKET/log/adhoc-sqls/$DATE/$NANO_SEC.sql" &> /dev/null
    SQL_FILES="s3://$APP_BUCKET/log/adhoc-sqls/$DATE/$NANO_SEC.sql"
    execSqlFiles
}

getJobStatus() {
    jobRunId="$1"
    aws glue get-job-run --region "$REGION" --job-name "$JOB_NAME" --run-id "$jobRunId" --query "JobRun.JobRunState" --output text
}

resolveDependentJars() {
    dependencyJarsList=""
    for jar in $SDL_HOME/lib/*.jar
    do
        dependencyJarsList="s3://$APP_BUCKET/lib/$(basename $jar),$dependencyJarsList"
    done
    echo "${dependencyJarsList%,}"
}

resolveConfigFiles() {
    configFilesList=""
    for file in $SDL_HOME/cnf/*
    do
        configFilesList="s3://$APP_BUCKET/cnf/$(basename $file),$configFilesList"
    done
    echo "${configFilesList%,}"
}

parseArgs() {
    if [ "$#" -eq 0 ]; then
        printUsage
        exit 0
    fi

    optString="job-name:,alias:,tags:,src-action:,src-table:,action:,sql:,sql-files:,sql-params:,sink-action:,sink-table:,hudi-record-key-field:,\
    hudi-precombine-field:,hudi-partition-path-field:"

    # IMPORTANT!! -o option can not be omitted, even there are no any short options!
    # otherwise, parsing will go wrong!
    OPTS=$(getopt -o "" -l "$optString" -- "$@")
    exitCode=$?
    if [ $exitCode -ne 0 ]; then
        echo ""
        printUsage
        exit 1
    fi
    eval set -- "$OPTS"
    while true; do
        case "$1" in
            --job-name)
                JOB_NAME="${2}"
                shift 2
                ;;
            --alias)
                JOB_ALIAS="${2}"
                shift 2
                ;;
            --tags)
                JOB_TAGS="${JOB_TAGS},${2}"
                shift 2
                ;;
            --src-action)
                SRC_ACTION="${2}"
                shift 2
                ;;
            --src-table)
                SRC_TABLE="$2"
                shift 2
                ;;
            --action)
                ACTION="$2"
                shift 2
                ;;
            --sql)
                SQL="$2"
                shift 2
                ;;
            --sql-files)
                SQL_FILES="$2"
                shift 2
                ;;
            --sql-params)
                SQL_PARAMS="$2"
                shift 2
                ;;
            --sink-action)
                SINK_ACTION="$2"
                shift 2
                ;;
            --sink-table)
                SINK_TABLE="$2"
                shift 2
                ;;
            --hudi-record-key-field)
                HUDI_RECORD_KEY_FIELD="$2"
                shift 2
                ;;
            --hudi-precombine-field)
                HUDI_PRECOMBINE_FIELD="$2"
                shift 2
                ;;
            --hudi-partition-path-field)
                HUDI_PARTITION_PATH_FIELD="$2"
                shift 2
                ;;
            --) # No more arguments
                shift
                break
                ;;
            *)
                echo ""
                echo "Invalid option $1." >&2
                printUsage
                exit 1
                ;;
        esac
    done
    shift $((OPTIND-1))
}

printUsage() {
    echo ""
    printHeading "USAGE"
    echo ""
    echo "SYNOPSIS"
    echo ""
    echo "$0 [ACTION] [--OPTION1 VALUE1] [--OPTION2 VALUE2]..."
    echo ""
    echo "ACTIONS:"
    echo ""
    echo "create                                create a job"
    echo "delete                                delete a job"
    echo "start                                 start a job"
    echo "exec-sql                              execute an ad-hoc sql"
    echo "exec-sql-files                        execute ad-hoc sql from sql files"
    echo "help                                  print usage"
    echo ""
    echo "OPTIONS:"
    echo ""
    echo "--job-name                            job name to be operate"
    echo "--alias                               job alias"
    echo "--tags                                tags for job"
    echo "--src-action                          action to be applied on source"
    echo "--src-table                           source table to be operated"
    echo "--action [sql|class]                  core action for etl operation, now support 2 types: sql or class"
    echo "--sql                                 sql to be executed"
    echo "--sql-files                           sql files to be executed"
    echo "--sql-params                          sql params, present as key-value pair string, i.e. year=2020,month=01"
    echo "--sink-action                         action to be applied on sink"
    echo "--sink-table                          sink table to be operated"
    echo "--hudi-record-key-field               hudi record key field for hudi table"
    echo "--hudi-precombine-field               hudi precombine field for hudi table"
    echo "--hudi-partition-path-field           hudi partition path field for hudi table"
    echo ""
    echo "EXAMPLES:"
    echo ""
    echo "# create job [ ods_tlc_yellow_trip_insert ] with tags"
    echo "$0 create --job-name ods_tlc_yellow_trip_insert --tags layer=ods,domain=tlc,action=etl"
    echo ""
    echo "# delete job [ ods_tlc_yellow_trip_insert ]"
    echo "$0 delete --job-name ods_tlc_yellow_trip_insert"
    echo ""
    echo "# start job [ ods_geo_taxi_zone_insert ] with parameters"
    echo "$0 start --job-name ods_geo_taxi_zone_insert --src-action load-csv-as-view --src-table stg.geo_taxi_zone --action sql --sql-files s3://$APP_BUCKET/sql/ods/geo/ods_geo_taxi_zone_insert.sql --sql-params year=2020,month=01 --sink-action save-view-to-hudi --sink-table ods.geo_taxi_zone --hudi-record-key-field locationid --hudi-precombine-field ts --hudi-partition-path-field year,month"
    echo ""
    echo "# execute an ad-hoc sql to drop table stg.pub_tmp"
    echo "$0 exec-sql --sql 'drop table if exists stg.pub_tmp'"
    echo ""
    echo "# execute an ad-hoc sql file drop all tmp tables on stg layer"
    echo "$0 exec-sql --sql-files s3://$APP_BUCKET/sql/cmn/drop_stg_tmp_tables.sql"
    echo ""
}

# -----------------------------------------------    Script Entrance    ---------------------------------------------- #

parseArgs "$@"

case $1 in
    (create)
        createJob
    ;;
    (delete)
        deleteJob
    ;;
    (start)
        startJob
    ;;
    (exec-sql)
        execSql
    ;;
    (exec-sql-files)
        execSqlFiles
    ;;
    (help)
        printUsage
    ;;
    (*)
        printUsage
    ;;
esac