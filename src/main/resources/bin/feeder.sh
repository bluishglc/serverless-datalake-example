#!/usr/bin/env bash

SDL_HOME="$(cd "`dirname $(readlink -nf "$0")`"/..; pwd -P)"

source "$SDL_HOME/bin/constants.sh"
source "$SDL_HOME/bin/util.sh"

YEAR=""
MONTH=""

CRAWLER_ARGS_FILE="$SDL_HOME/log/crawler-args/$CRAWLER_NAME.json"

# ----------------------------------------------    Script Functions    ---------------------------------------------- #

feedTlcData() {
    printHeading "FEED TLC DATA"
    # remove previous uploaded files
    rm -rf /tmp/nyc-tlc
    mkdir /tmp/nyc-tlc
    aws s3 rm "s3://$DATA_BUCKET/stg/tlc" --recursive

    for category in yellow green fhv fhvhv; do
        # wget "https://nyc-tlc.s3.amazonaws.com/trip data/${category}_tripdata_${YEAR}-${MONTH}.csv" -P "/tmp/nyc-tlc/"
        aws s3 cp "s3://nyc-tlc/csv_backup/${category}_tripdata_${YEAR}-${MONTH}.csv" "/tmp/nyc-tlc/"
        aws s3 cp "/tmp/nyc-tlc/${category}_tripdata_${YEAR}-${MONTH}.csv" "s3://$DATA_BUCKET/stg/tlc/${category}_trip/"
    done
}

feedGeoData() {
    printHeading "FEED GEO DATA"
    aws s3 rm "s3://$DATA_BUCKET/stg/geo" --recursive
    aws s3 cp "$SDL_HOME/dat/geo_dim_zone_${YEAR}-${MONTH}.csv" "s3://$DATA_BUCKET/stg/geo/taxi_zone/"
    echo -ne "a,b,c\n1,2,3" > /tmp/tmp.csv
    # make placeholder csv file to avoid inconsistent table naming in in case there is only one kind of data
    # for example, here, for taxi_zone, if no placeholder file, the crawled table name will be "geo_geo", not "geo_taxi_zone"
    aws s3 cp "/tmp/tmp.csv" "s3://$DATA_BUCKET/stg/geo/tmp/tmp.csv"
}

feedPubData() {
    printHeading "FEED PUB DATA"
    aws s3 rm "s3://$DATA_BUCKET/stg/pub" --recursive
    aws s3 cp "$SDL_HOME/dat/pub_dim_date.csv" "s3://$DATA_BUCKET/stg/pub/dim_date/"
    # make placeholder csv file to avoid inconsistent table naming in in case there is only one kind of data
    echo -ne "a,b,c\n1,2,3" > /tmp/tmp.csv
    aws s3 cp "/tmp/tmp.csv" "s3://$DATA_BUCKET/stg/pub/tmp/tmp.csv"
}

feedData() {
    printHeading "FEED DATA"
    feedPubData
    feedGeoData
    feedTlcData
}

parseArgs() {
    if [ "$#" -eq 0 ]; then
        printUsage
        exit 0
    fi

    optString="y:m:"
    longOptString="year:,month:"

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
            -y|--year)
                YEAR=$(printf "%04d" "${2}")
                shift 2
                ;;
            -m|--month)
                MONTH=$(printf "%02d" "${2}")
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
    echo "feed-data                             feed tlc/geo/pub data to staging layer"
    echo "feed-tlc-data                         feed tlc to staging layer"
    echo "feed-geo-data                         feed geo data to staging layer"
    echo "feed-pub-data                         feed pub data to staging layer"
    echo "help                                  print usage"
    echo ""
    echo "OPTIONS:"
    echo ""
    echo "-y|--year                             the year of dataset to feed"
    echo "-m|--month                            the month of dataset to feed"
    echo ""
    echo "EXAMPLES:"
    echo ""
    echo "# create all crawlers"
    echo "$0 batch-create"
    echo ""
    echo "# delete all crawlers"
    echo "$0 batch-delete"
    echo ""
    echo "# start all crawlers"
    echo "$0 batch-start"
    echo ""
}

# -----------------------------------------------    Script Entrance    ---------------------------------------------- #

parseArgs "$@"

case $1 in
    (feed-data)
        feedData
    ;;
    (feed-tlc-data)
        feedTlcData
    ;;
    (feed-geo-data)
        feedGeoData
    ;;
    (feed-pub-data)
        feedPubData
    ;;
    (help)
        printUsage
    ;;
    (*)
        printUsage
    ;;
esac

# leave an empty line
echo ""