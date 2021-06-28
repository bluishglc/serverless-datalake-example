#!/usr/bin/env bash

SDL_HOME="$(cd "`dirname $(readlink -nf "$0")`"/..; pwd -P)"

source "$SDL_HOME/bin/constants.sh"
source "$SDL_HOME/bin/ini.sh"
source "$SDL_HOME/bin/util.sh"

JOB_NAME=""
SQL_PARAMS=""
YEAR=""
MONTH=""
FROM=""
JOBS=""
LAYERS=""
DOMAINS=""
ACTIONS=""
ETL_ACTIONS=("insert" "upsert" "delete" "overwrite")

CATALOG_FILE="$SDL_HOME/cnf/catalog.json"

readJobs() {
    jq -rc '.[]' "$CATALOG_FILE" | while IFS='' read object; do
        layer=$(echo "$object" | jq -r .layer)
        domain=$(echo "$object" | jq -r .domain)
        jobsIniFile="$SDL_HOME/cnf/${layer}_${domain}.ini"
        if [ -f "$jobsIniFile" ]; then
            parseIni "$jobsIniFile"
            for jobName in "${ini_sections[@]}"; do
                action=${jobName##*_}
                if [[ "${LAYERS[@]}" == "" || "${LAYERS[@]}" =~ "$layer" ]] && \
                   [[ "${DOMAINS[@]}" == "" || "${DOMAINS[@]}" =~ "$domain" ]] && \
                   [[ "${ACTIONS[@]}" == "" || "${ACTIONS[@]}" =~ "$action" ]]; then
                    echo "$jobName"
                fi
            done
        fi
    done
}

printJobs() {
    printHeading "JOBS (IN CURRENT SESSION)"
    for job in "${JOBS[@]}"; do
        echo "$job"
    done
}

exists() {
    jobName="$1"
    result="false"
    for job in "${JOBS[@]}"; do
        if [ "$job" == "$jobName" ]; then
            result="true"
        fi
    done
    echo "$result"
}

create() {
    jobName="$1"
    if [ $(exists "$jobName") == "true" ]; then
        IFS='_' read -r -a array <<< "$jobName"
        layer=${array[0]}
        domain=${array[1]}
        action=${array[-1]}
        [[ ${ETL_ACTIONS[*]} =~ $action ]] && action="etl"
        $SDL_HOME/bin/job.sh create --job-name "$jobName" --tags "layer=$layer,domain=$domain,action=$action"
    else
        echo "ERROR! No such job in config files"
        exit 1
    fi
}

batchCreate() {
    printJobs
    for jobName in "${JOBS[@]}"; do
        create "$jobName"
    done
}

delete() {
    jobName="$1"
    if [ $(exists "$jobName") == "true" ]; then
        $SDL_HOME/bin/job.sh delete --job-name "$jobName"
    else
        echo "ERROR! No such job in config files"
        exit 1
    fi
}

batchDelete() {
    printJobs
    for jobName in "${JOBS[@]}"; do
        delete "$jobName"
    done
}

start() {
    jobName="$1"
    if [ $(exists "$jobName") == "true" ]; then
        IFS='_' read -r -a array <<< "$jobName"
        layer=${array[0]}
        domain=${array[1]}
        jobsIniFile="$SDL_HOME/cnf/${layer}_${domain}.ini"
        parseIni "$jobsIniFile"
        parseIniSection "$jobName"
        # if sqlFiles=auto-map in ini file, need resolve file paths
        if [ "$sqlFiles" == "auto-map" ]; then
            sqlFiles="$(resolveJobSqlFiles $jobName)"
        fi
        # if --sql-params is set, always pick cli's value, otherwise, use value in ini file
        sqlParams=${SQL_PARAMS:-${sqlParams}}
        $SDL_HOME/bin/job.sh start \
            --job-name "$jobName" \
            --src-action "$srcAction" \
            --src-table "$srcTable" \
            --action "$action" \
            --sql-files "$sqlFiles" \
            --sql-params "$sqlParams" \
            --sink-action "$sinkAction" \
            --sink-table "$sinkTable" \
            --hudi-record-key-field "$hudiRecordKeyField" \
            --hudi-precombine-field "$hudiPrecombineField" \
            --hudi-partition-path-field "$hudiPartitionPathField"
        # exit if jun failed!
        if [ "$?" != "0" ]; then exit 1; fi
    else
        echo "ERROR! No such job in config files"
        exit 1
    fi
}

batchStart() {
    printJobs
    for jobName in "${JOBS[@]}"; do
        start "$jobName"
        if [ "$?" != "0" ]; then exit 1; fi
    done
}

resolveJobSqlFiles() {
    jobName="$1"
    IFS='_' read -r -a array <<< "$jobName"
    # if sqlFiles is set in ini, pick it, otherwise, use resolved value according to job name
    echo "s3://$APP_BUCKET/sql/${array[0]}/${array[1]}/$jobName.sql"
}

parseArgs() {
    if [ "$#" -eq 0 ]; then
        printUsage
        exit 0
    fi

    optString="n:y:m:p:l:d:a:ec"
    longOptString="job-name:,year:,month:,sql-params:,layer:,domain:,action:,etl-jobs"

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
            -n|--job-name)
                JOB_NAME="${2}"
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
            -l|--layer)
                LAYERS+=("${2}")
                shift 2
                ;;
            -d|--domain)
                DOMAINS+=("${2}")
                shift 2
                ;;
            -a|--action)
                ACTIONS+=("${2}")
                shift 2
                ;;
            -e|--etl-jobs)
                ACTIONS+=(${ETL_ACTIONS[@]})
                shift 1
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
    echo "print-jobs                            print jobs"
    echo "create                                create a job predefined in ini file"
    echo "delete                                delete a job predefined in ini file"
    echo "start                                 start a job predefined in ini file"
    echo "batch-create                          create a batch of jobs predefined in ini file"
    echo "batch-delete                          delete a batch of jobs predefined in ini file"
    echo "batch-start                           start a a batch of jobs predefined in ini file"
    echo "help                                  print usage"
    echo ""
    echo "OPTIONS:"
    echo ""
    echo "-n|--job-name                         job name to be operate"
    echo "-y|--year                             the year of dataset to be processed"
    echo "-m|--month                            the month of dataset to be processed"
    echo "-p|--sql-params                       sql params, present as key-value pair string, i.e. year=2020,month=01"
    echo "-l|--layer                            layer of target jobs to be operated by batch"
    echo "-d|--domain                           domain of target jobs to be operated by batch"
    echo "-a|--action                           action type of target jobs to be operated by batch"
    echo "-e|--etl-jobs                         an action type set, including: insert, upsert, delete, overwrite"
    echo ""
    echo "EXAMPLES:"
    echo ""
    echo "# create job [ ods_tlc_yellow_trip_insert ] with tags"
    echo "$0 create --job-name ods_tlc_yellow_trip_insert"
    echo ""
    echo "# delete job [ ods_tlc_yellow_trip_insert ]"
    echo "$0 delete --job-name ods_tlc_yellow_trip_insert"
    echo ""
    echo "# start job [ ods_geo_taxi_zone_insert ] with parameters"
    echo "$0 start --job-name ods_geo_taxi_zone_insert --year 2020 --month 01"
    echo ""
    echo "# start all etl (insert, upsert, delete, overwrite) jobs of ods layer, tlc domain"
    echo "$0 batch-start --layer ods --domain tlc --etl-jobs --year 2020 --month 01"
    echo ""
    echo "# create all etl (insert, upsert, delete, overwrite) jobs in all layers and all domains"
    echo "$0 batch-start --etl-jobs --year 2020 --month 01"
    echo ""
}

# -----------------------------------------------    Script Entrance    ---------------------------------------------- #

parseArgs "$@"

JOBS=($(readJobs))

case $1 in
    print-jobs)
        printJobs
    ;;
    create)
        create "$JOB_NAME"
    ;;
    delete)
        delete "$JOB_NAME"
    ;;
    start)
        start "$JOB_NAME"
    ;;
    batch-create)
        batchCreate
    ;;
    batch-delete)
        batchDelete
    ;;
    batch-start)
        batchStart
    ;;
    help)
        printUsage
    ;;
    *)
        printUsage
    ;;
esac

# leave an empty line
echo ""
#printHeading "DONE"