#!/usr/bin/env bash

SDL_HOME="$(cd "`dirname $(readlink -nf "$0")`"/..; pwd -P)"

source "$SDL_HOME/bin/constants.sh"
source "$SDL_HOME/bin/util.sh"

CRAWLER_NAME=""
CRAWLER_TAGS="app=sdl"
DATABASE=""
TABLE_PREFIX=""
CRAWLER_ARGS_DIR="$SDL_HOME/log/crawler-args"

# ----------------------------------------------    Script Functions    ---------------------------------------------- #

createCrawler() {
    deleteCrawler
    printHeading "CREATING CRAWLER [ $CRAWLER_NAME ]"
    # make job args file...
    mkdir -p "$CRAWLER_ARGS_DIR"
    crawlerArgsFile="$CRAWLER_ARGS_DIR/$CRAWLER_NAME.json"
    cat <<-EOF > "$crawlerArgsFile"
    {
      "S3Targets": [
        {
          "Path": "s3://$DATA_BUCKET/$DATABASE/$TABLE_PREFIX/"
        }
      ]
    }
EOF
    aws glue create-crawler --region "$REGION" --role "$ROLE_ARN" \
        --name "$CRAWLER_NAME" \
        --database-name "$DATABASE" \
        --table-prefix "${TABLE_PREFIX}_" \
        --targets "file://$crawlerArgsFile" \
        --schema-change-policy "UpdateBehavior=UPDATE_IN_DATABASE,DeleteBehavior=LOG" \
        --configuration "{\"Version\":1.0,\"CrawlerOutput\":{\"Partitions\":{\"AddOrUpdateBehavior\":\"InheritFromTable\"},\"Tables\":{\"AddOrUpdateBehavior\":\"MergeNewColumns\"}}}" \
        --tags "${CRAWLER_TAGS:-""}"
    echo ""
    echo "The crawler [ $CRAWLER_NAME ] is [ CREATED ]"
}

deleteCrawler() {
    printHeading "DELETE CRAWLER [ $CRAWLER_NAME ]"
    # NOTE: use "aws glue get-crawler" will interrupt script executing not return non-zero value!
    # this is bad, so as use list-crawlers
    aws glue list-crawlers --region $REGION | jq -r '.CrawlerNames | .[]' | grep "$CRAWLER_NAME" &>/dev/null
    if [ "$?" = "0" ]; then
        aws glue delete-crawler --region $REGION --name $CRAWLER_NAME
        echo ""
        echo "The crawler [ $CRAWLER_NAME ] is [ DELETED ]"
    fi
}

startCrawler() {
    printHeading "START CRAWLER [ $CRAWLER_NAME ]"
    aws glue start-crawler --region "$REGION" --name "$CRAWLER_NAME"
    now=$(date +%s)sec
    while true; do
        crawlerStatus="$(aws glue get-crawler --region $REGION --name "$CRAWLER_NAME" --query "Crawler.State" --output text)"
        if [ ! "$crawlerStatus" == "READY" ]; then
            for i in {0..5}; do
                echo -ne "\E[33;5m>>> The crawler [ $CRAWLER_NAME ] is [ $crawlerStatus ], duration [ $(TZ=UTC date --date now-$now +%H:%M:%S) ] ....\r\E[0m"
                sleep 1
            done
        else
            echo -ne "\e[0A\e[KThe crawler [ $CRAWLER_NAME ] is [ $crawlerStatus ]\n"
            break
        fi
    done
}

parseArgs() {
    if [ "$#" -eq 0 ]; then
        printUsage
        exit 0
    fi

    optString="n:t:d:p"
    longOptString="crawler-name:,tags:,database:,table-prefix:"

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
            -n|--crawler-name)
                CRAWLER_NAME="${2}"
                shift 2
                ;;
            -t|--tags)
                CRAWLER_TAGS="${CRAWLER_TAGS},${2}"
                shift 2
                ;;
            -d|--database)
                DATABASE="${2}"
                shift 2
                ;;
            -p|--table-prefix)
                TABLE_PREFIX="${2}"
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
    echo "create                                create a crawler"
    echo "delete                                delete a crawler"
    echo "start                                 start a crawler"
    echo "help                                  print usage"
    echo ""
    echo "OPTIONS:"
    echo ""
    echo "-n|--crawler-name                     crawler name to be operate"
    echo "-t|--tags                             tags for crawler"
    echo "-d|--database                         target database of generated table by crawler"
    echo "-p|--table-prefix                     table name prefix of generated table by crawler"
    echo ""
    echo "EXAMPLES:"
    echo ""
    echo "# create crawler [ stg_tlc ] with tags"
    echo "$0 create --crawler-name stg_tlc --database stg --table-prefix tlc --tags layer=stg,domain=tlc"
    echo ""
    echo "# delete crawler [ stg_tlc ]"
    echo "$0 delete --crawler-name stg_tlc"
    echo ""
    echo "# start crawler [ stg_tlc ] with parameters"
    echo "$0 start --crawler-name stg_tlc"
    echo ""
}


# -----------------------------------------------    Script Entrance    ---------------------------------------------- #

parseArgs "$@"

case $1 in
    (create)
        createCrawler
    ;;
    (delete)
        deleteCrawler
    ;;
    (start)
        startCrawler
    ;;
    (help)
        printUsage
    ;;
    (*)
        printUsage
    ;;
esac

