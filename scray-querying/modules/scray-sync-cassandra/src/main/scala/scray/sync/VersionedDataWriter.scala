package scray.sync

import scray.sync.api.VersionedData
import scala.util.Try
import com.datastax.driver.core.Session
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.querybuilder.QueryBuilder

class VersionedDataWriter(host: String, keyspace: String, table: String, replicationSettings: Option[String]) {
  var cassandraSession: Session = null;
  
  def this(host: String, keyspace: String, table: String) = {
    this(host, keyspace, table, None)
  }
  
  def write(data: VersionedData): Try[Unit] = {
    Try({
      
      if(cassandraSession == null) {
        cassandraSession = Cluster.builder().addContactPoint(host).build().connect()
      }
      
      cassandraSession.execute(this.createKeyspaceCreationStatement(this.keyspace, replicationSettings))
      cassandraSession.execute(this.createTableStatement(table))

     val insertStatement = QueryBuilder.insertInto(keyspace, table)
        .value("dataSource", data.dataSource)
        .value("mergeKey",   data.mergeKey)
        .value("version",    data.version)
        .value("data",       data.data)
    
    cassandraSession.execute(insertStatement)
    })
  }
  
    def createTableStatement(tableName: String): String = {
    val createStatement = 
      s"CREATE TABLE IF NOT EXISTS ${keyspace + "." + tableName} (" +
      s"dataSource text, mergeKey text, version bigint, data text, " +
      s"PRIMARY KEY ((dataSource, version), mergeKey))" +
      s"WITH CLUSTERING ORDER BY (mergeKey ASC)"
     createStatement
  }

  def createKeyspaceCreationStatement(keyspace: String, replicationSettings: Option[String]): String = {
    
    if(replicationSettings.isDefined) {
      s"CREATE KEYSPACE IF NOT EXISTS ${keyspace} WITH REPLICATION = ${replicationSettings.get}"
    } else {
      s"CREATE KEYSPACE IF NOT EXISTS ${keyspace}"
    }
  }
  
}