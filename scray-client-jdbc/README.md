## Send Query ##
* Main class: 

		`scray.client.test.ScrayJdbcAccess`

* Programm arguments:

		`-u jdbc:scray:stateful://127.0.0.1:18181/cassandra/keyspace1/SIL -d -q "SELECT clientId FROM BISMTOlsWf WHERE clientId LIKE '*E*'  "`
