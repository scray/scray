#!/bin/bash

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
    echo "RUN_TYPE not set. Use default value service"
    SYNC_API_URL="http://ml-integration.research.dev.seeburger.de:8082/sync/versioneddata"
fi

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

  sftp -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa ubuntu@ml-integration-git.research.dev.seeburger.de:/home/ubuntu/sftp-share/$JOB_NAME.tar.gz ./$JOB_NAME.tar.gz
  tar -xzf $JOB_NAME.tar.gz
}

uploadCurrentNotebookState() {
  LOG_FILE=$1
  tar -czvf $JOB_NAME-state.tar.gz $SOURCE_DATA/$LOG_FILE
  sftp -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa  ubuntu@ml-integration-git.research.dev.seeburger.de:/home/ubuntu/sftp-share/ <<<'PUT '$JOB_NAME-state.tar.gz''
}

runPythonJob() {
  cd $JOB_LOCATION
  cd $SOURCE_DATA

  REQ_FILE=requirements.txt
 
  if test -f "$REQ_FILE"; then
    pip install -r requirements.txt 2>&1 | tee -a out.$JOB_NAME.txt 
  else
    echo "no requirements.txt"
  fi

#  mkfifo /tmp/python-job-out
#  < /tmp/python-job-out tee -a out.ff.txt &
#  python3 -u $NOTEBOOK_NAME &> /tmp/python-job-out & 
  echo "Execute: "  $NOTEBOOK_NAME 

  echo "python3 -u $NOTEBOOK_NAME 2>&1 | tee -a out.$JOB_NAME.txt" > run.sh
  chmod u+x run.sh
  ./run.sh &

  python3 $NOTEBOOK_NAME  2>&1 | tee -a out.$JOB_NAME.txt &
  uploadCurrentNotebookState out.$JOB_NAME.txt
  
  PID=$!


  echo "Wait for completion" >>  out.$JOB_NAME.txt
  tail out.$JOB_NAME.txt


  while ps -p $PID > /dev/null; do
    echo " python3 $NOTEBOOK_NAME $PID is running"
    echo "Upload std out"
    uploadCurrentNotebookState out.$JOB_NAME.txt
    sleep 40
  done

  tar -czvf $JOB_NAME-fin.tar.gz $SOURCE_DATA
  sftp -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa ubuntu@ml-integration-git.research.dev.seeburger.de:/home/ubuntu/sftp-share/ <<<'PUT '$JOB_NAME-fin.tar.gz''
}


runPapermillJob() {
  cd $JOB_LOCATION
  cd $SOURCE_DATA
  
  REQ_FILE=requirements.txt
 
  if test -f "$REQ_FILE"; then
    pip install -r requirements.txt 2>&1 | tee -a out.$JOB_NAME.txt 
  else
    echo "no requirements.txt"
  fi

  echo papermill --stdout-file notebook-stdout --autosave-cell-every 2  $NOTEBOOK_NAME out.$NOTEBOOK_NAME &

  papermill --stdout-file notebook-stdout --autosave-cell-every 2  $NOTEBOOK_NAME out.$NOTEBOOK_NAME &
  PID=$!

  uploadCurrentNotebookState out.$NOTEBOOK_NAME
  
  while ps -p $PID > /dev/null; do
    echo "papermill $PID is running"
    echo "Upload current notebook state"
    uploadCurrentNotebookState out.$NOTEBOOK_NAME
    sleep 40
  done

  tar -czvf $JOB_NAME-fin.tar.gz $SOURCE_DATA
  sftp -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa ubuntu@ml-integration-git.research.dev.seeburger.de:/home/ubuntu/sftp-share/ <<<'PUT '$JOB_NAME-fin.tar.gz''
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
  curl -X 'PUT' \
    $SYNC_API_URL'/latest' \
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
  STATE_OBJECT=$(curl -sS -X 'GET' $SYNC_API_URL'/latest?datasource='$JOB_NAME'&mergekey=_' -H 'accept: application/json' | jq '.data  | fromjson')
  STATE=$(echo "$STATE_OBJECT" | jq .state)
  SOURCE_DATA=$(echo "$STATE_OBJECT" | jq -r .dataDir)
  NOTEBOOK_NAME=$(echo "$STATE_OBJECT" | jq -r .notebookName)
  PROCESSING_ENV=$(echo "$STATE_OBJECT" | jq -r .processingEnv)

  echo SOURCE_DATA: "$SOURCE_DATA"
  echo NOTEBOOK_NAME: "$NOTEBOOK_NAME"
  echo PROCESSING_ENV: "$PROCESSING_ENV"

  while [ "$STATE" != "\"$TRIGGER_STATE\"" ]; do
    STATE_OBJECT=$(curl -sS -X 'GET' $SYNC_API_URL'/latest?datasource='$JOB_NAME'&mergekey=_' -H 'accept: application/json' | jq '.data  | fromjson')
    SOURCE_DATA=$(echo "$STATE_OBJECT" | jq -r .dataDir)
    NOTEBOOK_NAME=$(echo "$STATE_OBJECT" | jq -r .notebookName)

    STATE=$(echo "$STATE_OBJECT" | jq .state)
    echo "[$JOB_NAME] Wait for state $TRIGGER_STATE current state is " "$STATE"
    sleep 5
  done

  echo SOURCE_DATA: "$SOURCE_DATA"
  echo NOTEBOOK_NAME: "$NOTEBOOK_NAME"

  echo "State UPLOADED reached"
}


processNextJob() {
    waitForNextJob
    setState 'DOWNLOADING'
    downloadJob
    setState 'RUNNING'
    runJob
    setState 'COMPLETED'
}


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
