JOB_NAME=ki1-tensorflow-gpu
SOURCE_DATA=./
NOTEBOOK_NAME=token_classification_01.ipynb
INITIAL_STATE=""
PROCESSING_ENV=""
DOCKER_IMAGE="scrayorg/scray-jupyter_tensorflow-gpu:0.1.1"
JOB_NAME_LITERALLY=false
DATA_INTEGRATION_HOST=ml-integration-git.research.dev.example.com
DATA_INTEGRATION_USER=ubuntu
SYNC_API_URL="http://ml-integration.research.dev.example.com:8082"



createArchive() {
  echo "Create archive $JOB_NAME.tar.gz from source $SOURCE_DATA"
  tar -czvf $JOB_NAME.tar.gz $SOURCE_DATA > /dev/null
  sftp -o StrictHostKeyChecking=accept-new $DATA_INTEGRATION_USER@$DATA_INTEGRATION_HOST:sftp-share/  <<< 'put '$JOB_NAME'.tar.gz'
  rm -f ./$JOB_NAME.tar.gz
}

cleanUp() {
  # Remove old files
  rm -f ./$JOB_NAME-fin.tar.gz
  rm -f ./$JOB_NAME.tar.gz
  rm -f ./$JOB_NAME-state.tar.gz
  rm -f ./SYS-JOB-NAME-$JOB_NAME.json 
  rm -f out.$NOTEBOOK_NAME
}

downloadResuls() {
  rm -f $JOB_NAME-fin.tar.gz
  sftp $DATA_INTEGRATION_USER@$DATA_INTEGRATION_HOST:sftp-share/$JOB_NAME-fin.tar.gz ./
  tar -xzmf $JOB_NAME-fin.tar.gz

  # Clean up
  rm -f ./$JOB_NAME-fin.tar.gz
  rm -f ./$JOB_NAME.tar.gz
  rm -f ./$JOB_NAME-state.tar.gz
  rm -f ./SYS-JOB-NAME-$JOB_NAME.json 
  
  echo "Learning results loaded"
}

downloadUpdatedNotebook() {
  rm -f $JOB_NAME-state.tar.gz >/dev/null
  sftp $DATA_INTEGRATION_USER@$DATA_INTEGRATION_HOST:sftp-share/$JOB_NAME-state.tar.gz ./ &> /dev/null

  if [[ $? = 0 ]]; then
    tar -xzmf $JOB_NAME-state.tar.gz >/dev/null
    rm -f ./$JOB_NAME-state.tar.gz

    echo "Notebook out.$NOTEBOOK_NAME updated"
  fi

}

setState() {

  curl -sS --cacert ca.pem -X 'PUT' \
    ''$SYNC_API_URL'/sync/versioneddata/latest' \
    -H 'accept: */*' \
    -H 'Content-Type: application/json' \
    -H "$AUTH_HEADER" \
    -d '{
      "dataSource": "'$JOB_NAME'",
      "mergeKey": "_",
      "version": 0,
      "data": "{\"filename\": \"'$JOB_NAME'.tar.gz\", \"processingEnv\": \"'$PROCESSING_ENV'\", \"state\": \"'$1'\", \"imageName\": \"'$DOCKER_IMAGE'\",   \"dataDir\": \"'$SOURCE_DATA'\", \"notebookName\": \"'$NOTEBOOK_NAME'\"}",
      "versionKey": 0
    }'
}



getJobState() { 
  local http
  http=$(curl --cacert ca.pem -sS -w "%{http_code}" \
    -H "$AUTH_HEADER" \
    -H "accept: application/json" \
    -X GET \
    "$SYNC_API_URL/sync/versioneddata/latest?datasource=$JOB_NAME&mergekey=_" \
    -o response.json) || {
      echo "curl failed (network/SSL error)" >&2
      exit 1
    }

  if [[ $http -eq 401 || $http -eq 403 ]]; then
    echo "Authentication failed: invalid or expired token (HTTP $http)" >&2
    cat response.json >&2
    exit 1
  elif [[ $http -ge 400 ]]; then
    echo "Request failed with HTTP $http" >&2
    cat response.json >&2
    exit 1
  fi

  # Parse JSON and extract state
  if ! STATE_OBJECT=$(jq -e '.data | fromjson' response.json); then
    echo "Response JSON missing/invalid:" >&2
    cat response.json >&2
    exit 1
  fi

  STATE=$(jq -r '.state' <<<"$STATE_OBJECT")
}

waitForJobCompletion() {
  getJobState

  while [[ "$STATE" != "COMPLETED" ]]; do
    downloadUpdatedNotebook
    echo "Waiting for state COMPLETED current state: $STATE"
    sleep 8
    getJobState 
  done

  echo "State COMPLETED reached"
}

function parse-args() {

    while [ "$1" != "" ]; do
        case $1 in
            --job-name )   shift
                JOB_NAME=$1
        ;;
            --source-data )   shift
               SOURCE_DATA=$1
        ;;
            --notebook-name )   shift
                NOTEBOOK_NAME=$1
        ;;
            --initial-state )   shift
                INITIAL_STATE=$1
        ;;
	          --processing-env) shift
		            PROCESSING_ENV=$1
        ;;
	          --docker-image) shift
		            DOCKER_IMAGE=$1
	      ;;
	          --take-jobname-literally) shift
		            JOB_NAME_LITERALLY=$1            
        esac
        shift
    done
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
    echo "The environment variable SCRAY_SYNC_API_URL  not set. Default value \"$SYNC_API_URL\" is used."
else
    SYNC_API_URL="$SCRAY_SYNC_API_URL"
fi

if [ -z "$SCRAY_SYNC_API_TOKEN" ]; then
  echo "WARN: SCRAY_SYNC_API_TOKEN is not set. Please export your bearer token, e.g.:"
  echo "  export SCRAY_SYNC_API_TOKEN='your-token-here' For now default token is used"
  SCRAY_SYNC_API_TOKEN="super-secret-token"
fi
AUTH_HEADER="Authorization: Bearer $SCRAY_SYNC_API_TOKEN"

if [ "$1" == "run" ]
then
    shift
    parse-args "${@}" 

    if [ $JOB_NAME_LITERALLY == "false" ]
    then
      SYS_JOB_NAME=$JOB_NAME-$RANDOM 
      JOB_NAME=$SYS_JOB_NAME
    fi

    echo "{\"timestamp\": \"'$(date +%s)'\", \"jobName\": \"'$JOB_NAME'\", \"sysJobName\": \"'$SYS_JOB_NAME'\"}" > SYS-JOB-NAME-$JOB_NAME.json 
else         
    echo "Usage: run --job-name ki1-gpu --source-data token_classification --notebook-name token_classification_01.ipynb" 
    exit 1
fi

echo  JOB_NAME: $JOB_NAME SOURCE_DATA: $SOURCE_DATA NOTEBOOK_NAME: $NOTEBOOK_NAME

if [ "$INITIAL_STATE" == "RUNNING" ]
then
    waitForJobCompletion
elif [ "$INITIAL_STATE" == "COMPLETED" ]
then
   downloadResuls
else   
    cleanUp
    createArchive
    setState 'UPLOADED'
    waitForJobCompletion
    downloadResuls
fi
