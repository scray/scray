JOB_NAME=ki1-tensorflow-gpu
SOURCE_DATA=token_classification
NOTEBOOK_NAME=token_classification_01.ipynb

createArchive() {
  echo "Create archive $JOB_NAME.tar.gz from source $SOURCE_DATA"
  tar -czvf $JOB_NAME.tar.gz $SOURCE_DATA
  sftp -o StrictHostKeyChecking=accept-new ubuntu@ml-integration-git.research.dev.seeburger.de:/home/ubuntu/sftp-share/  <<< 'put '$JOB_NAME'.tar.gz'
}

downloadResuls() {
  rm -f $JOB_NAME-fin.tar.gz
  sftp ubuntu@ml-integration-git.research.dev.seeburger.de:/home/ubuntu/sftp-share/$JOB_NAME-fin.tar.gz ./
  tar -xzmf $JOB_NAME-fin.tar.gz

   echo "Learning results loaded"
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
  "data": "{\"filename\": \"'$JOB_NAME'.tar.gz\", \"state\": \"'$1'\",  \"dataDir\": \"'$SOURCE_DATA'\", \"notebookName\": \"'$NOTEBOOK_NAME'\"}",
  "versionKey": 0
}'

}


waitForJobCompletion() {
   STATE_OBJECT=$(curl -sS -X 'GET'   'http://ml-integration.research.dev.seeburger.de:8082/sync/versioneddata/latest?datasource='$JOB_NAME'&mergekey=_'   -H 'accept: application/json' | jq '.data  | fromjson')
   STATE=$(echo $STATE_OBJECT | jq .state)

  while [ "$STATE" != "\"COMPLETED\"" ]
  do
    STATE_OBJECT=$(curl -sS -X 'GET'   'http://ml-integration.research.dev.seeburger.de:8082/sync/versioneddata/latest?datasource='$JOB_NAME'&mergekey=_'   -H 'accept: application/json' | jq '.data  | fromjson')
    STATE=$(echo $STATE_OBJECT | jq .state)
    echo "Wait for state COMPLETED  current state is " $STATE
    sleep 5
  done

  echo "State COMPLETED reched"
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
        esac
        shift
    done
}


if [ "$1" == "run" ]
then
    shift
    parse-args "${@}"  
else         
    echo "Usage: run --job-name ki1-gpu --source-data token_classification --notebook-name token_classification_01.ipynb" 
    exit 1
fi

echo  JOB_NAME: $JOB_NAME SOURCE_DATA: $SOURCE_DATA NOTEBOOK_NAME: $NOTEBOOK_NAME

createArchive
setState 'UPLOADED'
waitForJobCompletion
downloadResuls