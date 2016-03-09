#!/bin/bash

ORIGDIR=$(cwd)

BASEDIR=$(dirname "$0")
cd $BASDIR/..

function usage() {
  echo "usage: $0 <Options> <Job arguments>\n\
--master <URL>    specify spark master. Required!\n\
"
}

# find spark-submit
if [ ! -z $SPARK_HOME ]; then
  SPARK_SUBMIT = "$SPARK_HOME/bin/spark-submit"
else
  SPARK_SUBMIT = $(which spark-submit)
fi

if [ -z "$SPARK_SUBMIT" ]; then
  echo "ERROR: Either have spark-submit on the PATH or SPARK_HOME must be set. Exiting."
  usage()
  exit 1
fi

# find spark master argument
while [[ $# > 0 ]]; do
  if [[ $1 == "--master" ]]; then
    SPARK_MASTER=$2
    shift 2
  elsif [[ $1 == "--help" ]]; then
    usage()
    exit 0
  else
    shift 1
  fi
done

if [ -z "$SPARK_MASTER" ]; then
  echo "ERROR: Need spark master URL to be specifies with --master <URL> . Exiting."
  usage()
  exit 2
fi

echo "Arguments: $@"

exec $SPARK_SUBMIT --master $SPARK_MASTER --class ${package}.${job-name} target/${artifactId}-${version}-jar-with-dependencies.jar $@

cd $ORIGDIR