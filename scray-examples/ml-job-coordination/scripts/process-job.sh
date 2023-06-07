#!/bin/bash
DEFAULT_JOB_NAME=ki1-tensorflow-gpu

if [ -z "$JOB_NAME" ]
then
      echo "JOB_NAME not set. Use default value $DEFAULT_JOB_NAME"
      JOB_NAME=$DEFAULT_JOB_NAME
fi

SOURCE_DATA=.
NOTEBOOK_NAME=example-notebook.ipynb
SYNC_API_URL="http://ml-integration.research.dev.seeburger.de:8082/sync/versioneddata"

downloadJob() {
  rm ./$JOB_NAME.tar.gz
  sftp ubuntu@ml-integration-git.research.dev.seeburger.de:/home/ubuntu/sftp-share/$JOB_NAME.tar.gz ./$JOB_NAME.tar.gz
  tar -xzf $JOB_NAME.tar.gz
}

uploadCurrentNotebookState() {
  tar -czvf $JOB_NAME-state.tar.gz $SOURCE_DATA/out.$NOTEBOOK_NAME
  sftp ubuntu@ml-integration-git.research.dev.seeburger.de:/home/ubuntu/sftp-share/ <<<'PUT '$JOB_NAME-state.tar.gz''
}


runJob() {
  cd $SOURCE_DATA

  papermill --stdout-file notebook-stdout --autosave-cell-every 2  $NOTEBOOK_NAME out.$NOTEBOOK_NAME &
  PID=$!

  while ps -p $PID > /dev/null; do
    echo "papermill $PID is running"
    echo "Upload current notebook state"
    uploadCurrentNotebookState
    sleep 4
  done

  if ["$SOURCE_DATA" != "./"]
  then
        cd ..
  fi
  tar -czvf $JOB_NAME-fin.tar.gz $SOURCE_DATA
  sftp ubuntu@ml-integration-git.research.dev.seeburger.de:/home/ubuntu/sftp-share/ <<<'PUT '$JOB_NAME-fin.tar.gz''
}

runLocalJob() {

  cd /mnt/ext-notebooks/
  papermill $NOTEBOOK_NAME out.$NOTEBOOK_NAME
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
    echo "[$JOB_NAME] Wait for state UPLOADED current state is " "$STATE"
    sleep 5
  done

  echo SOURCE_DATA: "$SOURCE_DATA"
  echo NOTEBOOK_NAME: "$NOTEBOOK_NAME"

  echo "State UPLOADED reached"
}


if [ "$SCRAY_SYNC_MODE" == "LOCAL" ]
then
    runLocalJob
    exit
else
  while true; do
    waitForNextJob

    setState 'DOWNLOADING'
    downloadJob
    setState 'RUNNING'
    runJob
    setState 'COMPLETED'

  done
fi
