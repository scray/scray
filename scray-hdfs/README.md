## Mapper for HDFS backends

Store to read key value pairs from SequenceFile

## Configuration parameters
* `store.conf`  
   Defines URL to blob and idx data. 

   ```
   connection hdfscluster hdfs {
   	url "hdfs://127.0.0.1:8020/user/scray/scray-hdfs-data/"
   }
   ```

* `~/scray-conf/queryspace.scray` 
    Define queryspace with name 000
    ```
	name 000 version 1
	table { hdfscluster, "testblobs", "ExampleData" }
  ```  
    
## Write Data

In this example we write 100 key value pairs in one SequenceFile.

```
object WriteExampleSequenceFile {
  def main(args: Array[String]) {

      val writer = new SequenceFileWriter(s" hdfs://127.0.0.1/user/scray/scray-hdfs-data/scray-data-${System.currentTimeMillis()}")

      for (i <- 0 to 100) {
        val key = "key_" + i
        val value = "data_" + i

        writer.insert(key, System.currentTimeMillis(), value.getBytes)
      }
      writer.close
    }
  }
```

This example can be found in `scray-hdfs-writer/src/main/scala/scray/hdfs/index/format/example/WriteExampleSequenceFile.scala` 
To execute this example

```
java -cp "scray-loader/target/lib/*:scray-hdfs-writer/target/scray-hdfs-writer-0.10.1-SNAPSHOT.jar" scray.hdfs.index.format.example.WriteExampleSequenceFile hdfs://127.0.0.1/user/scray/scray-hdfs-data/
```

## Start service
```
java -cp "scray-loader/target/lib/*:scray-loader/target/scray-loader-0.10.1-SNAPSHOT.jar" scray.loader.ScrayStandaloneService --config ~/scray-conf/store.conf
```

## Query data with CLI

Query data with id 'key_42' from dataset ExampleData 

```
java -cp "scray-loader/target/lib/*:scray-loader/target/scray-loader-0.10.1-SNAPSHOT.jar" scray.client.test.ScrayJdbcAccess -u jdbc:scray:stateful://127.0.0.1:18181/hdfscluster/testblobs/000 -q "SELECT * FROM ExampleData WHERE KEY='key_42'" -d
```


