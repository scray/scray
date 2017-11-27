#!/bin/bash

MY_PATH_REL="`dirname \"$0\"`"
MY_FULL_PATH="`( cd \"$MY_PATH_REL\" && pwd )`"

ORIGDIR=$(pwd)

BASEDIR=$(dirname $(readlink -f $0))
cd $BASEDIR/..

if [ ! -z $SPARK_HOME ]; then
 echo "WARN: SPARK_HOME variable ist set to $SPARK_HOME. This version will be used for spark-submit" 
fi

if [ ! -z $SPARK_SUBMIT ]; then
  echo "WARN: SPARK_SUBMIT variable is set to $SPARK_SUBMIT. This script will be used"
  usage
  exit 1
fi

SPARK_BINARY_URL="http://archive.apache.org/dist/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz"


EXTRACTED_SPARK_FOLDER_NAME="spark-2.2.0-bin-hadoop2.7"

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
