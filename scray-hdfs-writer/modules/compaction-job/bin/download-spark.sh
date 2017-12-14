#!/bin/bash

MY_PATH_REL="`dirname \"$0\"`"
MY_FULL_PATH="`( cd \"$MY_PATH_REL\" && pwd )`"

ORIGDIR=$(pwd)

BASEDIR=$(dirname $(readlink -f $0))
cd $BASEDIR/..

SPARK_VERSION=2.2.1
HADOOP_VERSION=2.7

if [ ! -z $SPARK_HOME ]; then
 echo "WARN: SPARK_HOME variable ist set to $SPARK_HOME. This version will be used for spark-submit" 
fi

if [ ! -z $SPARK_SUBMIT ]; then
  echo "WARN: SPARK_SUBMIT variable is set to $SPARK_SUBMIT. This script will be used"
  usage
  exit 1
fi

SPARK_BINARY_URL="http://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz"

EXTRACTED_SPARK_FOLDER_NAME="spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION"

echo "SPARK_HOME="$MY_FULL_PATH/$EXTRACTED_SPARK_FOLDER_NAME
export SPARK_HOME=$MY_FULL_PATH/$EXTRACTED_SPARK_FOLDER_NAME

echo "YARN_CONF_DIR="$MY_FULL_PATH/../conf
export YARN_CONF_DIR=$MY_FULL_PATH/../conf


function downloadSparkBinaries {
        echo $SPARK_BINARY_URL
        wget $SPARK_BINARY_URL -O $MY_FULL_PATH/spark.tgz && mv $MY_FULL_PATH/spark.tgz $MY_FULL_PATH/../lib/
        tar -xvzf $MY_FULL_PATH/../lib/spark.tgz -C $MY_FULL_PATH/../lib/
}

if [ ! -e "$MY_FULL_PATH/spark.tgz" ]
then
	downloadSparkBinaries  
else 
	echo "Spark already exists. No download"
fi
