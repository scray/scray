package scray.cassandra.util

import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration.StoreColumnFamily
import com.datastax.driver.core.{ KeyspaceMetadata, Metadata }

object CassandraUtils {

  def getKeyspaceMetadata(cf: StoreColumnFamily): KeyspaceMetadata = 
    cf.session.getSession.getCluster().getMetadata().getKeyspace(Metadata.quote(cf.session.getKeyspacename))
   
  def getTableMetadata(cf: StoreColumnFamily, km: Option[KeyspaceMetadata] = None) = {
    val kspaceMeta = km match {
      case Some(ksm) => ksm
      case None => getKeyspaceMetadata(cf)
    }
    kspaceMeta.getTable(cf.getPreparedNamed)
  }
}