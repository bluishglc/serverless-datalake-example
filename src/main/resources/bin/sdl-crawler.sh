#!/usr/bin/env bash

SDL_HOME="$(cd "`dirname $(readlink -nf "$0")`"/..; pwd -P)"

source "$SDL_HOME/bin/constants.sh"
source "$SDL_HOME/bin/util.sh"

LAYER=""
DOMAIN=""

CATALOG_FILE="$SDL_HOME/cnf/catalog.json"

readCrawlers() {
    jq -rc '.[]' "$CATALOG_FILE" | while IFS='' read object; do
        layer=$(echo "$object" | jq -r .layer)
        domain=$(echo "$object" | jq -r .domain)
        creator=$(echo "$object" | jq -r .creator)
        if [[ "$creator" == "glue-crawler" ]] && \
           [[ "${LAYERS[@]}" == "" || "${LAYERS[@]}" =~ "$layer" ]] && \
           [[ "${DOMAINS[@]}" == "" || "${DOMAINS[@]}" =~ "$domain" ]]; then
            echo "${layer}_${domain}"
        fi
    done
}

batchCreate() {
    for crawler in "${CRAWLERS[@]}"; do
        IFS='_' read -r -a array <<< "$crawler"
        layer=${array[0]}
        domain=${array[1]}
        $SDL_HOME/bin/crawler.sh create --crawler-name "${layer}_${domain}" --database "${layer}" --table-prefix "${domain}" --tags "layer=$layer,domain=$domain"
    done
}

batchDelete() {
    for crawler in "${CRAWLERS[@]}"; do
        IFS='_' read -r -a array <<< "$crawler"
        layer=${array[0]}
        domain=${array[1]}
        $SDL_HOME/bin/crawler.sh delete --crawler-name "${layer}_${domain}"
    done
}

batchStart() {
    for crawler in "${CRAWLERS[@]}"; do
        IFS='_' read -r -a array <<< "$crawler"
        layer=${array[0]}
        domain=${array[1]}
        $SDL_HOME/bin/crawler.sh start --crawler-name "${layer}_${domain}"
    done
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
    echo "batch-create                          create all crawlers by batch"
    echo "delete                                delete all crawlers by batch"
    echo "start                                 start all crawlers by batch"
    echo "help                                  print usage"
    echo ""
    echo "OPTIONS:"
    echo ""
    echo "-l|--layer                            layer of target crawlers to be operated by batch"
    echo "-d|--domain                           domain of target crawlers to be operated by batch"
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

parseArgs() {
    if [ "$#" -eq 0 ]; then
        printUsage
        exit 0
    fi

    optString="l:d:"
    longOptString="layer:,domain:"

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
            -l|--layer)
                LAYERS+=("${2}")
                shift 2
                ;;
            -d|--domain)
                DOMAINS+=("${2}")
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

# -----------------------------------------------    Script Entrance    ---------------------------------------------- #

parseArgs "$@"

CRAWLERS=($(readCrawlers))

case $1 in
    (list)
        list
    ;;
    (batch-create)
        batchCreate
    ;;
    (batch-delete)
        batchDelete
    ;;
    (batch-start)
        batchStart
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
#printHeading "DONE"