## Start Service ##
* Main class: `scray.loader.ScrayStandaloneService`
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

    queryspacelocations {
            url "file:///home/otto/scray-conf/queryspace.scray"
    }
```

* `/home/otto/scray-conf/queryspace.scray` File
```
	name SIL version 1
	
	table { cassandra, "keyspace1", "ColumnFamily1" }
	
	materialized_view table { cassandra, "keyspace1", "ColumnFamily2" }, keygeneratorClass: "scray.common.key.OrderedStringKeyGenerator"
```
	
## Send Query ##
* Main class: `scray.client.test.ScrayJdbcAccess`
* Programm arguments:
		`-u jdbc:scray:stateful://127.0.0.1:18181/cassandra/keyspace1/SIL -d -q "SELECT clientId FROM BISMTOlsWf WHERE clientId LIKE '*E*'  "`
