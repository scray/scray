## Configure Hosts
In this example we assume that the folowing hostnames are available on all hosts  


|Hostname|Example IP|
|---|---|
|hdfs-namenode|10.0.0.1|
|yarn.resourcemanager|10.0.0.1|
|kafka-broker-1|10.0.0.1|
|graphite-host|10.0.0.1|

## Prepare Job
   
* Download external libraries. In this case Spark 2.2.0  
 
    ```
    ~/git/scray/scray-example/spark-job/facility-state-job/bin/download-spark.sh
    ```

* Build job
    ```
    cd ~/git/scray/scray-example/spark-job/facility-state-job/
    mvn package
    ```
 
* Job parameter  
   Parameters are configured in ```conf/facility-state-job-local.yaml``` for local execution or ```conf/facility-state-job.yaml``` if this job is executed in YARN cluster.
   See details in [Manual configuration](#jobManualConf)

## Start job on local host
  Spark master and worker are starting automatically 
```
cd ~/git/scray/scray-example/spark-job/facility-state-job/
./bin/submit-job.sh --local-mode --total-executor-cores 10
```

## Start job in YARN cluster 
  A running YARN cluster is required.  
  Some HADOOP specific parameters are defined in ```conf/core-site.xml``` and ```yarn-site.xml```
```
cd ~/git/scray/scray-example/spark-job/facility-state-job/
./bin/submit-job.sh  yarn-cluster --total-executor-cores 3
    
```

## Implementation
   Facility data are read from an kafka stream and parsed to Facilty objects.
   This objects are counted depending on a time window and the type and state of the facility.
   All aggregated data are written to a graphite host

   Details are described in the sourcode file [here](https://github.com/scray/scray/blob/feature/report-example/scray-example/spark-job/facility-state-job/src/main/scala/org/scray/example/SparkSQLStreamingJob.scala)  
   For details of the window syntax please refer to [Apache Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time)

<a name="jobManualConf"></a>
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
* **Configure window parameter**  
   Facility data are aggregated depending a predefined window.
   This window parameters can be configured:
   ```    
   windowDuration: 20
   slideDuration: 40
   watermark: 0
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