package scray.cassandra.automation

import com.twitter.storehaus.cassandra.cql.CQLCassandraRowStore
import com.websudos.phantom.CassandraPrimitive
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration.StoreColumnFamily
import scray.cassandra.util.CassandraUtils
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.BatchStatement
import com.twitter.util.Duration

/**
 * a factory for rows stores which reads table Metadata from Cassandra and builds CQLCassandraRowStores
 */
object RowStoreFactory {
  import scala.collection.JavaConverters._
  
  /**
   * create a row store object using metadata extraction of the datastax java driver
   */
  def getRowStore[K](cf: StoreColumnFamily,
	  consistency: ConsistencyLevel = CQLCassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
	  poolSize: Int = CQLCassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
	  batchType: BatchStatement.Type = CQLCassandraConfiguration.DEFAULT_BATCH_STATEMENT_TYPE,
	  ttl: Option[Duration] = CQLCassandraConfiguration.DEFAULT_TTL_DURATION)(
	      implicit typeMap: Map[String, CassandraPrimitive[_]]): (Option[CQLCassandraRowStore[K]], List[(String, CassandraPrimitive[_])]) = {
    // retrieve table metadata
    val tableMeta = CassandraUtils.getTableMetadata(cf)
    val metaInfo = tableMeta.getColumns().asScala.map(colMeta => 
      (colMeta.getName(), typeMap.get(colMeta.getType().getName().toString()).get)).toList
    val keyColumnNames = tableMeta.getPartitionKey().asScala
    keyColumnNames.size match {
      case x if x == 1 =>
        // create store
        val keySerializer = typeMap.get(keyColumnNames(0).getType().getName().toString()).get.asInstanceOf[CassandraPrimitive[K]]
        (Some(new CQLCassandraRowStore[K](cf, metaInfo, keyColumnNames(0).getName(), consistency, poolSize, batchType, ttl)(keySerializer)), metaInfo) 
      case _ => (None, metaInfo)
    }
  }
}
