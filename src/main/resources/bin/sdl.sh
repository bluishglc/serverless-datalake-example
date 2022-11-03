#!/usr/bin/env bash

SDL_HOME="$(cd "`dirname $(readlink -nf "$0")`"/..; pwd -P)"

source "$SDL_HOME/bin/constants.sh"
source "$SDL_HOME/bin/util.sh"
source "$SDL_HOME/bin/athena.sh"

YEAR=""
MONTH=""
FROM=""
SQL_PARAMS=""
SKIP_FEED_DATA="false"

CATALOG_FILE="$SDL_HOME/cnf/catalog.json"
TABLES=$(jq -r '.[] | .tables | .[]' "$CATALOG_FILE")

createGlueInfra() {
    $SDL_HOME/bin/job.sh create --job-name "$ADHOC_SQL_JOB"
    # for prd env, it is advised to create etl jobs only,
    # creating table jobs are unsafe and unnecessary。
    # $SDL_HOME/bin/sdl-job.sh batch-create --etl-jobs
    $SDL_HOME/bin/sdl-job.sh batch-create
    $SDL_HOME/bin/sdl-crawler.sh batch-create
}

deleteGlueInfra() {
    $SDL_HOME/bin/job.sh delete --job-name "$ADHOC_SQL_JOB"
    $SDL_HOME/bin/sdl-job.sh batch-delete
    $SDL_HOME/bin/sdl-crawler.sh batch-delete
}

createSchema() {
    printHeading "CREATE DATALAKE SCHEMA"
    # create databases tables of datalake
    $SDL_HOME/bin/job.sh exec-sql-files --alias "create_all_databases_and_tables" \
    --sql-files "s3://$APP_BUCKET/sql/cmn/create_databases.sql,s3://$APP_BUCKET/sql/*/*/*_create.sql"
    # must feed some data before crawling, otherwise no create on stg.
    if [ "$SKIP_FEED_DATA" != "true" ]; then
        $SDL_HOME/bin/feeder.sh feed-data --year "2020" --month "01"
    fi
    $SDL_HOME/bin/sdl-crawler.sh batch-start
    $SDL_HOME/bin/job.sh exec-sql-files --alias "drop_stg_tmp_tables" \
    --sql-files "s3://$APP_BUCKET/sql/cmn/drop_stg_tmp_tables.sql"
}

build() {
    # feed data of given year, month
    # uncomment when release
    if [ "$SKIP_FEED_DATA" != "true" ]; then
        $SDL_HOME/bin/feeder.sh feed-data --year "$YEAR" --month "$MONTH"
    fi
    # run monthly etl build
    $SDL_HOME/bin/sdl-job.sh batch-start --etl-jobs --year "$YEAR" --month "$MONTH"
}

cleanData() {
    printHeading "CLEAN ALL DATA"
    aws s3 rm --recursive s3://$DATA_BUCKET/ods/
    aws s3 rm --recursive s3://$DATA_BUCKET/dwh/
    aws s3 rm --recursive s3://$DATA_BUCKET/dmt/
    aws s3 rm --recursive s3://$DATA_BUCKET/tmp/
}

showData() {
    for table in $TABLES; do
        execAthenaSql "select * from $table limit 1"
    done
}

deploy() {
    printHeading "DEPLOY SERVERLESS DATALAKE"

    # create app bucket if not exists
    aws s3 ls s3://$APP_BUCKET &> /dev/null
    if [ ! "$?" == 0 ]; then
        aws s3 mb s3://$APP_BUCKET
    fi

    # delete existing app files
    aws s3 rm "s3://$APP_BUCKET/" --recursive
    # upload resources
    aws s3 sync "$SDL_HOME/" "s3://$APP_BUCKET/" --exclude "log/*"

    # create data bucket if not exists
    aws s3 ls s3://$DATA_BUCKET &> /dev/null
    if [ ! "$?" == 0 ]; then
        aws s3 mb s3://$DATA_BUCKET
    fi

    # upload workflow files to MWAA (Airflow)
    aws s3 ls $AIRFLOW_DAGS_HOME &> /dev/null
    if [ ! "$?" == 0 ]; then
        echo "WARNING: The configured dir [ $AIRFLOW_DAGS_HOME ] for MWAA (Airflow) DAG files does NOT exist!"
        echo "Please make sure if you have an available MWAA environment and configured right dir path!"
    else
        aws s3 sync "$SDL_HOME/wfl/" "$AIRFLOW_DAGS_HOME/"
    fi
}

init() {
    deploy
    cleanData
    createGlueInfra
    createSchema
}

parseArgs() {
    if [ "$#" -eq 0 ]; then
        printUsage
        exit 0
    fi

    optString="s::p:l:d:y:m:"
    longOptString="skip-feed-data::,sql-params:,layer:,domain:,year:,month:"

    # IMPORTANT!! -o option can not be omitted, even there are no any short options!
    # otherwise, parsing will go wrong!
    OPTS=$(getopt -o "$optString" -l "$longOptString" -- "$@")
    exitCode=$?
    if [ $exitCode -ne 0 ]; then
        echo ""
        printUsage
        exit 1
    fi
    eval set -- "$OPTS"
    while true; do
        case "$1" in
            -s|--skip-feed-data)
                SKIP_FEED_DATA="true"
                shift 2
                ;;
            -y|--year)
                YEAR=$(printf "%04d" "${2}")
                shift 2
                ;;
            -m|--month)
                MONTH=$(printf "%02d" "${2}")
                shift 2
                ;;
            -p|--sql-params)
                SQL_PARAMS="${2}"
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
    FROM="${YEAR}${MONTH}01000000"
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
    echo "init                                  initialise datalake, including:"
    echo "                                      1. deploy project from local to s3"
    echo "                                      2. clean existing data if exist"
    echo "                                      3. create glue infrastructure, including jobs and crawlers"
    echo "                                      4. start table creating job to init schema"
    echo "deploy                                deploy project from local to s3"
    echo "create-glue-infra                     create glue infrastructure, including jobs and crawlers"
    echo "delete-glue-infra                     delete glue infrastructure, including jobs and crawlers"
    echo "build                                 build a cycle (monthly) of jobs, run all etl jobs of a cycle"
    echo "show-data                             show all tables' data (limit 10 records)"
    echo "help                                  print usage"
    echo ""
    echo "OPTIONS:"
    echo ""
    echo "-s|--skip-feed-data                     skip feeding raw data, this is used for re-run jobs without re-loading data from remote"
    echo "-y|--year                             the year of dataset to be processed"
    echo "-m|--month                            the month of dataset to be processed"
    echo "-p|--sql-params                       sql params, present as key-value pair string, i.e. year=2020,month=01"
    echo ""
    echo "EXAMPLES:"
    echo ""
    echo "# initialise datalake"
    echo "$0 init"
    echo ""
    echo "# deploy project from local to s3"
    echo "$0 deploy"
    echo ""
    echo "# create glue infrastructure, including jobs and crawlers"
    echo "$0 create-glue-infra"
    echo ""
    echo "# delete glue infrastructure, including jobs and crawlers"
    echo "$0 delete-glue-infra"
    echo ""
    echo "# build 2020-01 cycle (monthly) of jobs, run all etl jobs of cycle 2020-01"
    echo "$0 build --year 2020 --month 01"
    echo ""
}

# -----------------------------------------------    Script Entrance    ---------------------------------------------- #

parseArgs "$@"

case $1 in
    (init)
        init
    ;;
    (deploy)
        deploy
    ;;
    (create-glue-infra)
        createGlueInfra
    ;;
    (delete-glue-infra)
        deleteGlueInfra
    ;;
    (build)
        shift
        build "$@"
    ;;
    (show-data)
        shift
        showData
    ;;
    (help)
        printUsage
    ;;
    (*)
        printUsage
    ;;
esac