# Prepare Job

To download Spark 2.2.0 execute ```~/git/scray/scray-example/spark-job/facility-state-job/bin/download-spark.sh```

Set env vars for spark-submit.sh

```
export SPARK_HOME=~/git/scray/scray-example/spark-job/facility-state-job/lib/spark-2.2.0-bin-hadoop2.7
export YARN_CONF_DIR=~/git/scray/scray-example/spark-job/facility-state-job/conf
export HADOOP_USER_NAME=hdfs
```

## Start Spark shell
```
export SPARK_LOCAL_IP=192.168.42.22
./bin/spark-shell --master yarn --deploy-mode client --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 

```
