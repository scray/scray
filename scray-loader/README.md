## Start Service ##
* Main class: 

    ```scray.loader.ScrayStandaloneService```

* Programm arguments:

    ```--config /home/otto/scray-conf/store.conf```
* `store.conf` File    

```
    service {
            advertise host "127.0.0.1",
            service port 18181,
    }

    connection cassandra cassandra {
            hosts ("127.0.0.1"),
            datacenter "datacenter1",
            clustername "Test Cluster"
    }

    connection oracle jdbc {
            url "jdbc:oracle:thin:DBUSR1/Pw12@10.1.1.1:1521:SCRAY",
            credentials "DBUSR1" : "Pw12"
    }

    connection hdfscluster hdfs {
        url "hdfs://10.1.1.1:8020/user/scray/scray-hdfs-data/"
    }

    queryspacelocations {
            url "file:///home/otto/scray-conf/queryspace.scray"
    }
```

* `~/scray-conf/queryspace.scray` File
```
	name SIL version 1

	table { cassandra, "keyspace1", "ColumnFamily1" }
	table { cassandra, "keyspace1", "ColumnFamily2" }
	table { cassandra, "keyspace1", "ColumnFamily3" }
	table { oracle,    "SCRAY",     "Table1" }
	table { hdfscluster, "blobrefs", "Elementbuffers" }	
	materialized_view table { cassandra, "keyspace1", "ColumnFamily2" }, keygeneratorClass: "scray.common.key.OrderedStringKeyGenerator"
```

* Example:
```
cd ~/git/scray
mvn clean install
java -cp "scray-loader/target/lib/*:scray-loader/target/scray-loader-0.10.1.jar" scray.loader.ScrayStandaloneService --config /home/otto/scray-conf/store.conf
``` 
