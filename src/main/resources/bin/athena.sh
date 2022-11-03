#!/usr/bin/env bash

execAthenaSql() {
    sql="$1"
    printHeading "EXECUTE SQL [ $sql ] ON ATHENA"
    queryExecutionId=$(aws athena \
        start-query-execution \
        --region "$REGION" \
        --query-string "$sql" \
        --query-execution-context Catalog=AwsDataCatalog \
        --result-configuration OutputLocation=s3://$APP_BUCKET/athena-query-results/ \
        --query "QueryExecutionId" \
        --output text
    )
    while true; do
        queryStatus="$(aws athena get-query-execution --query-execution-id "$queryExecutionId" --region "$REGION" --query "QueryExecution.Status.State" --output text)"
        if [ ! "$queryStatus" == "SUCCEEDED" ] && [ ! "$queryStatus" == "FAILED" ]; then
            echo -ne "\E[33;5m>>> The query [ $queryExecutionId ] state is [ $queryStatus ], duration [ $(TZ=UTC date --date now-$now +%H:%M:%S) ] ....\r\E[0m"
            sleep 1
        else
            echo -ne "\e[0A\e[KThe query [ $queryExecutionId ] is [ $queryStatus ]\n"
            break
        fi
    done
    # disable aws client pager
    export AWS_PAGER=""
    aws athena get-query-results --query-execution-id "$queryExecutionId" --region "$REGION" --output table
}

execAthenaSqlFile() {
    sqlFile="$1"
    sql="$(cat $sqlFile)"
    execAthenaSql "$sql"
}
