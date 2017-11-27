## Configure Hosts
In this example we assume that the folowing hostnames are available on all hosts  


|Hostname|Example IP|
|---|---|
|hdfs-namenode|10.0.0.1|
|yarn.resourcemanager|10.0.0.1|
|kafka-broker-1|10.0.0.1|
|graphite-host|10.0.0.1|

## Prepare Job
   
* Dowload external libraries. In this case Spark 2.2.0  
 
    ```
    ~/git/scray/scray-example/spark-job/facility-state-job/bin/download-spark.sh
    ```

* Set environment variable for spark-submit.sh

    ```
    export SPARK_HOME=~/git/scray/scray-example/spark-job/facility-state-job/lib/spark-2.2.0-bin-hadoop2.7
    export YARN_CONF_DIR=~/git/scray/scray-example/spark-job/facility-state-job/conf
    export HADOOP_USER_NAME=hdfs
    ``` 

* Build job
    ```
    cd =~/git/scray/scray-example/spark-job/facility-state-job/
    mvn package
    ```  

## Start job  
```
cd ~/git/scray/scray-example/spark-job/facility-state-job/
./bin/submit-job.sh  --master yarn-cluster --total-executor-cores 3
    
```

## Manual configuration

### Job source and sink configuration

* **Facility data are consumed from an Kafka broker**   
    Kafka broker is configured in ```conf/facility-state-job.conf```   
    E.g.
  
    ```
    "kafkaBootstrapServers": "192.0.2.1:9092",
    "kafkaTopic": "facility",
    ```
* **In this example results are written to Graphite**  
    Graphite hosts in configured in ```conf/facility-state-job.conf```  
    E.g.

    ```
    "graphiteHost":  "192.0.2.1"
    ```

### Execution environement configuration  

* **In this example YARN is used to execute  the facility-state-job** 

    YARN hosts are configured in ```yarn-site.xml```  
    E.g.   
 ```
<configuration>
        <property>
                <name>yarn.resourcemanager.resource-tracker.address</name>
                <value>yarn.resourcemanager:8025</value>
        </property>
        <property>
                <name>yarn.resourcemanager.scheduler.address</name>
                <value>192.0.2.1:8030</value>
        </property>
        <property>
                <name>yarn.resourcemanager.address</name>
                <value>192.0.2.1:8032</value>
        </property>
        <property>
                <name>yarn.resourcemanager.webapp.address</name>
                <value>192.0.2.1:8089</value>
        </property>
</configuration>
```

* **Data like runtime libraries and job definition are stored in HDFS** 
    HDFS hosts are configured in ```conf/core-site.xml```  
    E.g.
  ```
  <property>
          <name>fs.defaultFS</name>
          <value>hdfs://192.0.2.1:8020</value>
          <description>NameNode URI</description>
  </property>
  ```
