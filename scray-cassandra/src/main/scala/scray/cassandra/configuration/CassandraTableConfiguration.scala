package scray.cassandra.configuration

import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration.StoreColumnFamily
import scray.cassandra.util.CassandraUtils
import com.twitter.util.Try

/**
 * logic to define and process table configurations written into Cassandra column families comments
 */
class CassandraTableConfiguration {
  
  /**
   * property name of the property which holds the parallelization used to read and write the column family 
   */
  val PARALLELIZATION_TABLE_PROPERTY = "index_parallelization"
  
  /**
   * return an Option with the function to get the 
   */
  def parallelizationFunction(cf: StoreColumnFamily): () => Option[Int] = 
    () => CassandraUtils.getTablePropertyFromCassandra(cf, PARALLELIZATION_TABLE_PROPERTY).flatMap(prop => Try(prop.toInt).toOption)

}