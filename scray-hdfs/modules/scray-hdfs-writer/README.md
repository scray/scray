## Features
  * Store multiple key value pairs in one SequenceFile
  * Read and write data with java.io.InputStream
  * Api to store String and Array[Byte] data
  * Store huge binary files (internally data are splitted and merged while writing/reading)

### Example: Write JSON data and query data with Apache Hive
  Create Hive table:
  
    CREATE EXTERNAL TABLE scray3 (
      msg_id      BIGINT,
      msg         STRING
     )
     ROW FORMAT SERDE "org.apache.hive.hcatalog.data.JsonSerDe"
     WITH SERDEPROPERTIES (
       "msg_id"="$.id",
       "msg"="$.created_at"
     )
     STORED AS
     INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat'
     OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'
  
  Write data to HDFS:

    val writer = new TextSequenceFileWriter("hdfs://hdfs.scray.org/user/hive/warehouse/scray/json")
      
    writer.insert("id1", """{"msg_id": 1, "msg": "msg1"}""")
    writer.insert("id2", """{"msg_id": 2, "msg": "msg2"}""")
    writer.insert("id3", """{"msg_id": 3, "msg": "msg3"}""")
      
    writer.close

  Query data with Hive:
  
    SELECT * FROM scray
    SELECT count(*) FROM scray
    SELECT * FROM scray WHERE msg='msg1'

  If **org.apache.hive.hcatalog.data.JsonSerDe** is missing set *HIVE_AUX_JARS_PATH=/opt/local/hive/lib* 
  and copy hive-hcatalog-core-X.X.X.jar to /opt/local/hive/lib [See](https://www.cloudera.com/documentation/enterprise/5-3-x/topics/cm_mc_hive_udf.html)

