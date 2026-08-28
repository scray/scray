#!/bin/bash

DATA_INTEGRATION_HOST=ml-integration-git.research.dev.example.com
DATA_INTEGRATION_USER=ubuntu
SYNC_API_URL="http://ml-integration.research.dev.example.com:8082"
OUTPUT_FOLDER="job_output"
RUNNING_STATE="RUNNING"
RESUMABLE_JOB="${RESUMABLE_JOB:-true}"

if [[ -z "${TRIGGER_STATE}" ]]; then
  echo "TRIGGER_STATE not set use default \"SCHEDULED\""
  TRIGGER_STATE="SCHEDULED"
fi


DEFAULT_JOB_NAME=ki1-tensorflow-gpu

if [[ -z "${RUNTIME_TYPE}" ]]; then
  echo "RUNTIME_TYPE not set use default PAPERMILL"
  RUNTIME_TYPE="PAPERMILL"
fi

echo RUNTIME_TYPE=$RUNTIME_TYPE

if [ -z "$JOB_NAME" ]
then
      echo "JOB_NAME not set. Use default value $DEFAULT_JOB_NAME"
      JOB_NAME=$DEFAULT_JOB_NAME
fi

if [ -z "$RUN_TYPE" ]
then
    echo "RUN_TYPE not set. Use default value service"
    RUN_TYPE=service
fi

echo "RUN_TYPE is: $RUN_TYPE"

if [ -z "$SYNC_API_URL" ]
then
    echo "SYNC_API_URL not set. Use default value service "
    SYNC_API_URL="http://ml-integration.research.dev.example.com:8082/sync/versioneddata"
fi

if [ -z "$SCRAY_SYNC_API_TOKEN" ]; then
  echo "WARN: SCRAY_SYNC_API_TOKEN is not set. Please export your bearer token, e.g.:"
  echo "  export SCRAY_SYNC_API_TOKEN='your-token-here' For now default token is used"
  SCRAY_SYNC_API_TOKEN="super-secret-token"
fi
AUTH_HEADER="Authorization: Bearer $SCRAY_SYNC_API_TOKEN"


SOURCE_DATA=.
NOTEBOOK_NAME=example-notebook.ipynb
JOB_LOCATION="~/jobs/b636f6f92d51e742f861ee2a928621b6/"

prepareSshEnv() {
  mkdir ~/.ssh
  cp /etc/ssh-key/id_rsa ~/.ssh/id_rsa
  chmod 600 ~/.ssh/id_rsa
}

downloadJob() {

  # Prepare environment
  cd ~/
  mkdir -p jobs
  cd jobs
  JOB_FOLDER=$(echo -n $JOB_NAME | md5sum | cut -f1 -d" ")
  echo "Job folder: $JOB_FOLDER"

  rm -fr $JOB_FOLDER
  mkdir -p $JOB_FOLDER
  cd $JOB_FOLDER
  JOB_LOCATION=$(pwd)

  sftp -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa $DATA_INTEGRATION_USER@$DATA_INTEGRATION_HOST:sftp-share/$JOB_NAME.tar.gz ./$JOB_NAME.tar.gz
  tar -xzf $JOB_NAME.tar.gz
}

uploadCurrentNotebookState() {
  LOG_FOLDER=$1
  
  local ARCHIVE="${JOB_NAME}-state.tar.gz"

  tar -czvf "$ARCHIVE" -C "$LOG_FOLDER" .
  sftp -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa \
    "$DATA_INTEGRATION_USER@$DATA_INTEGRATION_HOST:sftp-share/" <<EOF
put "$ARCHIVE"
EOF
}

runPythonJob() {
  cd $JOB_LOCATION
  cd $SOURCE_DATA

  mkdir -p $OUTPUT_FOLDER

  REQ_FILE=requirements.txt
 
  if test -f "$REQ_FILE"; then
    pip install -r requirements.txt 2>&1 | tee -a $OUTPUT_FOLDER/out.pip.$JOB_NAME.log 
  else
    echo "no requirements.txt"
  fi

#  mkfifo /tmp/python-job-out
#  < /tmp/python-job-out tee -a out.ff.txt &
#  python3 -u $NOTEBOOK_NAME &> /tmp/python-job-out & 
  echo "Execute: "  $NOTEBOOK_NAME 

 # echo "python3 -u $NOTEBOOK_NAME 2>&1 | tee -a out.$JOB_NAME.txt" > run.sh
 # chmod u+x run.sh
 # ./run.sh &

  python3 $NOTEBOOK_NAME  2>&1 | tee -a $OUTPUT_FOLDER/out.$JOB_NAME.log &
  uploadCurrentNotebookState $OUTPUT_FOLDER
  
  PID=$!

  echo "Wait for completion" >>  $OUTPUT_FOLDER/out.$JOB_NAME.log
  tail $OUTPUT_FOLDER/out.$JOB_NAME.log


  while ps -p $PID > /dev/null; do
    echo " python3 $NOTEBOOK_NAME $PID is running"
    echo "Upload std out"
    uploadCurrentNotebookState $OUTPUT_FOLDER
    sleep 40
  done

  uploadCurrentNotebookState $OUTPUT_FOLDER
  tar -czvf $JOB_NAME-backup.tar.gz $OUTPUT_FOLDER/ 
  sftp -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa $DATA_INTEGRATION_USER@$DATA_INTEGRATION_HOST:sftp-share/ <<<'PUT '$JOB_NAME-backup.tar.gz''
}


runPapermillJob() {
  cd $JOB_LOCATION
  cd $SOURCE_DATA
 
  mkdir -p $OUTPUT_FOLDER

  REQ_FILE=requirements.txt
 
  if test -f "$REQ_FILE"; then
    pip install -r requirements.txt 2>&1 | tee -a $OUTPUT_FOLDER/out.pip.$JOB_NAME.txt 
  else
    echo "no requirements.txt"
  fi
  echo "Joblocationn $JOB_LOCATION"

  papermill --stdout-file $OUTPUT_FOLDER/notebook-stdout.txt --stderr-file $OUTPUT_FOLDER/notebook-stderr.txt --autosave-cell-every 2  $NOTEBOOK_NAME $OUTPUT_FOLDER/out.$NOTEBOOK_NAME &
  PID=$!

  uploadCurrentNotebookState $OUTPUT_FOLDER
  
  while ps -p $PID > /dev/null; do
    echo "papermill $PID is running"
    echo "Upload current notebook state"
    uploadCurrentNotebookState $OUTPUT_FOLDER
    sleep 40
  done
 
  uploadCurrentNotebookState $OUTPUT_FOLDER
  tar -czvf $JOB_NAME-fin.tar.gz $OUTPUT_FOLDER/ 
  sftp -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa $DATA_INTEGRATION_USER@$DATA_INTEGRATION_HOST:sftp-share/ <<<'PUT '$JOB_NAME-backup.tar.gz''
}

runJob() {
  
  if [ "$RUNTIME_TYPE" == "PAPERMILL" ]
  then
   runPapermillJob
  elif [ "$RUNTIME_TYPE" == "PYTHON" ]
  then
    runPythonJob
  else
    echo "Process one job."
    processNextJob
  fi

EXECUTION_ENV=

}

runLocalJob() {

  cd /mnt/ext-notebooks/
  papermill $NOTEBOOK_NAME out.$NOTEBOOK_NAME
}

setState() {
  echo $1
  curl -k -X 'PUT' \
    $SYNC_API_URL'/latest' \
    -H "$AUTH_HEADER" \
    -H 'accept: */*' \
    -H 'Content-Type: application/json' \
    -d '{
  "dataSource": "'$JOB_NAME'",
  "mergeKey": "_",
  "version": 0,
  "data": "{\"filename\": \"'$JOB_NAME'.tar.gz\", \"processingEnv\": \"'$PROCESSING_ENV'\",  \"state\": \"'$1'\",  \"dataDir\": \"'$SOURCE_DATA'\", \"notebookName\": \"'$NOTEBOOK_NAME'\"}",
  "versionKey": 0
}'

}

waitForNextJob() {
  STATE_OBJECT=$(curl -k -sS -H "$AUTH_HEADER" -X 'GET' $SYNC_API_URL'/latest?datasource='$JOB_NAME'&mergekey=_' -H 'accept: application/json' | jq '.data  | fromjson')
  STATE=$(echo "$STATE_OBJECT" | jq -r '.state')
  SOURCE_DATA=$(echo "$STATE_OBJECT" | jq -r .dataDir)
  NOTEBOOK_NAME=$(echo "$STATE_OBJECT" | jq -r .notebookName)
  PROCESSING_ENV=$(echo "$STATE_OBJECT" | jq -r .processingEnv)

  echo SOURCE_DATA: "$SOURCE_DATA"
  echo NOTEBOOK_NAME: "$NOTEBOOK_NAME"
  echo PROCESSING_ENV: "$PROCESSING_ENV"

  if [ "$STATE" = "$RUNNING_STATE" ]; then
    if [ "$RESUMABLE_JOB" = "true" ]; then
      echo "Job is resumable. Restarting interrupted job."
      setState 'RESTARTING'
      return 0
    else
      echo "Job is not resumable. Marking it as externally terminated."
      setState 'EXTERNALLY_TERMINATED'
      return 1
    fi
  else
    while [ "$STATE" != "$TRIGGER_STATE" ]; do
      STATE_OBJECT=$(curl -k -sS -H "$AUTH_HEADER" -X GET \
        "$SYNC_API_URL/latest?datasource=$JOB_NAME&mergekey=_" \
        -H 'accept: application/json' |
        jq '.data | fromjson')

      SOURCE_DATA=$(echo "$STATE_OBJECT" | jq -r '.dataDir')
      NOTEBOOK_NAME=$(echo "$STATE_OBJECT" | jq -r '.notebookName')
      STATE=$(echo "$STATE_OBJECT" | jq -r '.state')

      echo "[$JOB_NAME] Wait for state $TRIGGER_STATE; current state is $STATE"
      sleep 5
    done
  fi

  echo SOURCE_DATA: "$SOURCE_DATA"
  echo NOTEBOOK_NAME: "$NOTEBOOK_NAME"

  echo "State "$TRIGGER_STATE" reached"
}


processNextJob() {
    waitForNextJob
    setState 'DOWNLOADING'
    downloadJob
    setState $RUNNING_STATE
    runJob
    setState 'COMPLETED'
}


# Check if sync host env var is empty
if [ -z "$SCRAY_DATA_INTEGRATION_HOST" ]; then
    echo "The environment variable  SCRAY_DATA_INTEGRATION_HOST not set. Default value \"$DATA_INTEGRATION_HOST\" is used."
else
    DATA_INTEGRATION_HOST="$SCRAY_DATA_INTEGRATION_HOST"
fi

# Check if sync host user env var is empty
if [ -z "$SCRAY_DATA_INTEGRATION_USER" ]; then
    echo "The environment variable  SCRAY_DATA_INTEGRATION_USER not set. Default value \"$DATA_INTEGRATION_USER\" is used."
else
    DATA_INTEGRATION_USER="$SCRAY_DATA_INTEGRATION_USER"
fi

# Check if sync host user env var is empty
if [ -z "$SCRAY_SYNC_API_URL" ]; then
    echo "The environment variable SCRAY_SYNC_API_URL not set. Default value \"$SYNC_API_URL\" is used."
else
    SYNC_API_URL="$SCRAY_SYNC_API_URL/sync/versioneddata"
fi


if [ "$SCRAY_SYNC_MODE" == "LOCAL" ]
then
    runLocalJob
    exit
else
  prepareSshEnv
  if [ "$RUN_TYPE" == "service" ]
  then
    while true; do
      processNextJob
    done
  else
    echo "Process one job."
    processNextJob
  fi
fi

echo "Job $JOB_NAME completed. Terminate job processor"
