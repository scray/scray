JOB_NAME=ki1-tensorflow-gpu
SOURCE_DATA=./
NOTEBOOK_NAME=token_classification_01.ipynb
INITIAL_STATE=""
PROCESSING_ENV=""
DOCKER_IMAGE="huggingface-transformers-pytorch-deepspeed-latest-gpu-dep:0.1.2"
JOB_NAME_LITERALLY=false

createArchive() {
  echo "Create archive $JOB_NAME.tar.gz from source $SOURCE_DATA"
  tar -czvf $JOB_NAME.tar.gz $SOURCE_DATA
  sftp -o StrictHostKeyChecking=accept-new ubuntu@ml-integration-git.research.dev.seeburger.de:/home/ubuntu/sftp-share/  <<< 'put '$JOB_NAME'.tar.gz'
}

downloadResuls() {
  rm -f $JOB_NAME-fin.tar.gz
  sftp ubuntu@ml-integration-git.research.dev.seeburger.de:/home/ubuntu/sftp-share/$JOB_NAME-fin.tar.gz ./
  tar -xzmf $JOB_NAME-fin.tar.gz
  rm -f $JOB_NAME-fin.tar.gz
  
  echo "Learning results loaded"
}

downloadUpdatedNotebook() {
  rm -f $JOB_NAME-state.tar.gz >/dev/null
  sftp ubuntu@ml-integration-git.research.dev.seeburger.de:/home/ubuntu/sftp-share/$JOB_NAME-state.tar.gz ./ &> /dev/null

  if [[ $? = 0 ]]; then
    tar -xzmf $JOB_NAME-state.tar.gz >/dev/null
    echo "Notebook out.$NOTEBOOK_NAME updated"
  fi

}


setState() {
echo $1
curl -sS -X 'PUT' \
  'http://ml-integration.research.dev.seeburger.de:8082/sync/versioneddata/latest' \
  -H 'accept: */*' \
  -H 'Content-Type: application/json' \
  -d '{
  "dataSource": "'$JOB_NAME'",
  "mergeKey": "_",
  "version": 0,
  "data": "{\"filename\": \"'$JOB_NAME'.tar.gz\", \"processingEnv\": \"'$PROCESSING_ENV'\", \"state\": \"'$1'\", \"imageName\": \"'$DOCKER_IMAGE'\",   \"dataDir\": \"'$SOURCE_DATA'\", \"notebookName\": \"'$NOTEBOOK_NAME'\"}",
  "versionKey": 0
  }'
}

waitForJobCompletion() {
   STATE_OBJECT=$(curl -sS -X 'GET'   'http://ml-integration.research.dev.seeburger.de:8082/sync/versioneddata/latest?datasource='$JOB_NAME'&mergekey=_'   -H 'accept: application/json' | jq '.data  | fromjson')

  while [ "$STATE" != "\"COMPLETED\"" ]
  do
    STATE_OBJECT=$(curl -sS -X 'GET'   'http://ml-integration.research.dev.seeburger.de:8082/sync/versioneddata/latest?datasource='$JOB_NAME'&mergekey=_'   -H 'accept: application/json' | jq '.data  | fromjson')
    STATE=$(echo "$STATE_OBJECT" | jq .state)

    downloadUpdatedNotebook

    echo "Wait for state COMPLETED  current state is " "$STATE"
    sleep 8
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
    createArchive
    setState 'UPLOADED'
    waitForJobCompletion
    downloadResuls
fi
