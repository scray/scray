#!/bin/bash

JOB_NAME=randomforest
SOURCE_DATA=default
NOTEBOOK_NAME=default.ipynb
SYNC_API_URL="http://ml-integration.research.dev.seeburger.de:8082/sync/versioneddata"

downloadJob() {
  rm ./$JOB_NAME.tar.gz
  sftp ubuntu@ml-integration-git.research.dev.seeburger.de:/home/ubuntu/sftp-share/$JOB_NAME.tar.gz ./$JOB_NAME.tar.gz
  tar -xzf $JOB_NAME.tar.gz
}

runJob() {

  cd $SOURCE_DATA
  papermill $NOTEBOOK_NAME out.$NOTEBOOK_NAME

  cd ..
  tar -czvf $JOB_NAME-fin.tar.gz $SOURCE_DATA
  sftp ubuntu@ml-integration-git.research.dev.seeburger.de:/home/ubuntu/sftp-share/ <<<'PUT '$JOB_NAME-fin.tar.gz''
}

setState() {
  echo $1
  curl -X 'PUT' \
    $SYNC_API_URL'/latest' \
    -H 'accept: */*' \
    -H 'Content-Type: application/json' \
    -d '{
  "dataSource": "'$JOB_NAME'",
  "mergeKey": "_",
  "version": 0,
  "data": "{\"filename\": \"'$JOB_NAME'\", \"state\": \"'$1'\"}",
  "versionKey": 0
}'

}

waitForNextJob() {
  STATE_OBJECT=$(curl -sS -X 'GET' $SYNC_API_URL'/latest?datasource='$JOB_NAME'&mergekey=_' -H 'accept: application/json' | jq '.data  | fromjson')
  STATE=$(echo "$STATE_OBJECT" | jq .state)
  SOURCE_DATA=$(echo "$STATE_OBJECT" | jq -r .dataDir)
  NOTEBOOK_NAME=$(echo "$STATE_OBJECT" | jq -r .notebookName)

  echo SOURCE_DATA: "$SOURCE_DATA"
  echo NOTEBOOK_NAME: "$NOTEBOOK_NAME"

  while [ "$STATE" != "\"UPLOADED\"" ]; do
    STATE_OBJECT=$(curl -sS -X 'GET' $SYNC_API_URL'/latest?datasource='$JOB_NAME'&mergekey=_' -H 'accept: application/json' | jq '.data  | fromjson')
    SOURCE_DATA=$(echo "$STATE_OBJECT" | jq -r .dataDir)
    NOTEBOOK_NAME=$(echo "$STATE_OBJECT" | jq -r .notebookName)

    STATE=$(echo "$STATE_OBJECT" | jq .state)
    echo "Wait for state UPLOADED current state is " "$STATE"
    sleep 5
  done

  echo SOURCE_DATA: "$SOURCE_DATA"
  echo NOTEBOOK_NAME: "$NOTEBOOK_NAME"

  echo "State UPLOADED reached"
}

while true; do
  # setState 'UPLOADED'
  waitForNextJob

  setState 'DOWNLOADING'
  downloadJob
  setState 'RUNNING'
  runJob
  setState 'COMPLETED'

done
