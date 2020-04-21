#!/bin/bash

BINDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASEDIR=`eval "cd $BINDIR/../;pwd;cd - > /dev/null"`

SPARK_VERSION=2.3.2
HADOOP_VERSION=2.7
SPARK_BINARY_URL="http://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz"

EXTRACTED_SPARK_FOLDER_NAME="spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION"



if [ ! -z $SPARK_HOME ]; then
 echo "WARN: SPARK_HOME variable is set to $SPARK_HOME. This version will be used for spark-submit" 
fi

if [ ! -z $SPARK_SUBMIT ]; then
  echo "WARN: SPARK_SUBMIT variable is set to $SPARK_SUBMIT. This script will be used"
  usage
  exit 1
fi



SPARK_HOME=$BASEDIR/lib/$EXTRACTED_SPARK_FOLDER_NAME
echo "SPARK_HOME="$SPARK_HOME
export SPARK_HOME=$SPARK_HOME

YARN_CONF_DIR=$BASEDIR/conf
echo "YARN_CONF_DIR="$YARN_CONF_DIR
export YARN_CONF_DIR=$YARN_CONF_DIR


function downloadSparkBinaries {
        echo $SPARK_BINARY_URL
        wget $SPARK_BINARY_URL -O $BASEDIR/lib/spark.tgz
        tar -xvzf $BASEDIR/lib/spark.tgz -C $BASEDIR/lib/
}

if [ ! -e "$SPARK_HOME" ]
then
	downloadSparkBinaries  
else 
	echo "Spark already exists. No download"
fi
