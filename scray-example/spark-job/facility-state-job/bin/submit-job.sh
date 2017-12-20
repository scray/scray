#!/bin/bash

ORIGDIR=$(pwd)

BASEDIR=$(dirname $(readlink -f $0))
cd $BASEDIR/..

function usage {
  echo -e "Usage: $0 <Options> <Job arguments>\n\
Options:\n\
  --master <URL>                    specify spark master. Required!\n\
  --total-executor-cores <NUMBER>   number of cores to use. Required!\n\
  --help                            display this usage information\n\
"
}

# find spark-submit
if [ ! -z $SPARK_HOME ]; then
  SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
else
  SPARK_SUBMIT=$(which spark-submit)
fi

if [ -z "$SPARK_SUBMIT" ]; then
  echo "ERROR: Either have spark-submit on the PATH or SPARK_HOME must be set. Exiting."
  usage
  exit 1
fi

ARGUMENTS=()
# find spark master argument
while [[ $# > 0 ]]; do
  if [[ $1 == "--master" ]]; then
    SPARK_MASTER=$2
    shift 2
  elif [[ $1 == "--total-executor-cores" ]]; then
    CORES=$2
    shift 2
  elif [[ $1 == "--help" ]]; then
    usage
    exit 0
  else
    ARGUMENTS+=("$1")
    shift 1
  fi
done

if [ -z "$SPARK_MASTER" ]; then
  echo "ERROR: Need spark master URL to be specifies with --master <URL> . Exiting."
  usage
  exit 2
fi

if [ -z "$CORES" ]; then
  echo "ERROR: Need number of cores to be specifies at cli with --total-executor-cores <NUMBER> . Exiting."
  usage
  exit 3
fi

# exec $SPARK_SUBMIT --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 --master $SPARK_MASTER --deploy-mode cluster --driver-memory 512m --executor-memory 512m --total-executor-cores $CORES  --files $BASEDIR/../conf/log4j.properties,$BASEDIR/../conf/facility-state-job.yaml --class org.scray.example.FacilityStateJob target/facility-state-job-1.0-SNAPSHOT-jar-with-dependencies.jar ${ARGUMENTS[@]}

exec $SPARK_SUBMIT --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 --master $SPARK_MASTER --driver-memory 512m --executor-memory 512m --total-executor-cores $CORES  --files $BASEDIR/../conf/log4j.properties,$BASEDIR/../conf/facility-state-job-local.yaml --class org.scray.example.FacilityStateJob target/facility-state-job-1.0-SNAPSHOT-jar-with-dependencies.jar ${ARGUMENTS[@]}



cd $ORIGDIR
