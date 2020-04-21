#!/bin/bash

BINDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASEDIR=`eval "cd $BINDIR/../;pwd;cd - > /dev/null"`

source $BASEDIR/bin/download-spark.sh

# Set default values
LOCAL_MODE=false
SPARK_MASTER="spark://127.0.0.1:7077"
CORES=2


function usage {
  echo -e "Usage: $0 <Options> <Job arguments>\n\
Options:\n\
  --local-mode			    instantiate and use local Spark.\n\
  --master <URL>                    specify spark master. Required!\n\
  --total-executor-cores <NUMBER>   number of cores to use. Required!\n\
  --help                            display this usage information\n\
"
}


ARGUMENTS=()
# find spark master argument
while [[ $# > 0 ]]; do
  if [[ $1 == "--master" ]]; then
    SPARK_MASTER=$2
    shift 2
  elif [[ $1 == "--total-executor-cores" ]]; then
    CORES=$2
    shift 2
  elif [[ $1 == "--local-mode" ]]; then
    LOCAL_MODE=true
    shift 1 
  elif [[ $1 == "--help" ]]; then
    usage
    exit 0
  else
    ARGUMENTS+=("$1")
    shift 1
  fi
done

# find spark-submit

if [ $LOCAL_MODE = false ]; then
  if [ ! -z $SPARK_HOME ]; then
    SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
  else
    SPARK_SUBMIT=$(which spark-submit)
  fi
echo ff
  if [ -z "$SPARK_SUBMIT" ]; then
    echo "ERROR: Either have spark-submit on the PATH or SPARK_HOME must be set. Exiting."
    usage
    exit 1
  fi
  
  if [ -z "$SPARK_MASTER" ]; then
    echo "ERROR: Need spark master URL to be specifies with --master <URL> . Exiting."
    usage
    exit 2
  fi
else
  SPARK_SUBMIT=$BASEDIR/lib/$EXTRACTED_SPARK_FOLDER_NAME/bin/spark-submit
fi


if [ -z "$CORES" ]; then
  CORES=2
  echo "INFO: Number of cores is not specified at cli. Use default value --total-executor-cores $CORES. "
fi

if [ $LOCAL_MODE = true  ]; then
  echo "Start local Spark"
  SPARK_MASTER_HOST=127.0.0.1
  export "SPARK_MASTER_HOST="$SPARK_MASTER_HOST
  echo $SPARK_HOME/sbin/start-master.sh
  $SPARK_HOME/sbin/start-master.sh
  $SPARK_HOME/sbin/start-slave.sh $SPARK_MASTER_HOST:7077
  exec $SPARK_SUBMIT --master $SPARK_MASTER --total-executor-cores $CORES --files $BASEDIR/conf/log4j.properties,$BASEDIR/conf/job-parameter.json --class com.seeburger.research.cloud.ai.IngestSlaSqlData target/ingest-sql-sla-data-1.0-SNAPSHOT-jar-with-dependencies.jar ${ARGUMENTS[@]}
else
  exec $SPARK_SUBMIT --master $SPARK_MASTER --total-executor-cores $CORES --files $BASEDIR/conf/log4j.properties,$BASEDIR/conf/job-parameter.json --class com.seeburger.research.cloud.ai.IngestSlaSqlData target/ingest-sql-sla-data-1.0-SNAPSHOT-jar-with-dependencies.jar ${ARGUMENTS[@]}
fi
