package scray.cassandra.automation

import com.twitter.storehaus.QueryableStore
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration.{ StoreColumnFamily, StoreSession }
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.websudos.phantom.CassandraPrimitive
import scray.cassandra.rows.GenericCassandraRowStoreMapper
import scray.querying.description.{ Row, TableIdentifier, VersioningConfiguration }
import scray.querying.storeabstraction.StoreGenerators
import scray.querying.sync.cassandra.CassandraDbSession
import scray.querying.sync.types.DbSession
import scray.querying.storeabstraction.StoreExtractor
import com.twitter.storehaus.ReadableStore
import scray.cassandra.extractors.CassandraExtractor
import com.twitter.storehaus.cassandra.cql.AbstractCQLCassandraStore

/**
 * Generators for Scray store abstractions for Cassandra
 */
class CassandraStoreGenerators(dbID: String, session: DbSession[_, _, _], 
    cassSessionHandler: CassandraSessionHandler)(implicit typeMaps: Map[String, CassandraPrimitive[_]]) extends StoreGenerators with LazyLogging {
  
  lazy val casssession = session match {
    case cassandra: CassandraDbSession => cassandra 
    case _ => // this should never happen...
        throw new RuntimeException("Store session is not a Cassandra Session. This is a bug. Please report.")  
  }
  
//  lazy val casscluster = transformSessionToCluster(casssession)
//  
  override def createRowStore(table: TableIdentifier): 
      Option[(QueryableStore[_, _], ((_) => Row, Option[String], Option[VersioningConfiguration[_, _, _]]))] = {
    val TableIdentifier(db, keyspace, cfname) = table 
    if (db == dbID) {
      val casscluster = casssession.cassandraSession.getCluster
      val resolvedsession = cassSessionHandler.getSession((dbID, keyspace), casscluster)
      val store = RowStoreFactory.getRowStore[String](
          StoreColumnFamily(cfname, resolvedsession), poolSize = 50)._1.get
      Some((store, (GenericCassandraRowStoreMapper.rowMapper(store, None), None, None)))
    } else {
      None
    }}.asInstanceOf[Option[(QueryableStore[_, _], (_ => Row, Option[String], Option[VersioningConfiguration[_, _, _]]))]]


  override def getExtractor[S <: QueryableStore[_, _]](store: S, tableName: Option[String], 
      versions: Option[VersioningConfiguration[_, _, _]]): StoreExtractor[S] = {
    store match {
      case cqlStore: AbstractCQLCassandraStore[k, v] => CassandraExtractor.getExtractor(cqlStore, tableName, versions).asInstanceOf[StoreExtractor[S]] 
      case _ => throw new UnsupportedOperationException("CassandraStoreGenerators can only be used with Cassandra stores")
    }
    
  }
//  /**
//   * transforms a DBSession object back to the older StoreCluster
//   * TODO: get rid of Storehaus and remove this
//   */
//  private def transformSessionToCluster(casssession: CassandraDbSession): StoreCluster = {
//    import scala.collection.convert.decorateAsScala._
//    val name = casssession.cassandraSession.getCluster.getClusterName
//    val hosts = casssession.cassandraSession.getCluster.getMetadata.
//        getAllHosts.asScala.map { x => StoreHost(x.getAddress.getHostAddress) }.toSet
//    val creds = 
//        
//    StoreCluster(name, hosts, /* credentials */ credentials,
//                          loadBalancing,
//                          reconnectPolicy,
//                          retryPolicy,
//                          shutdownTimeout,
//                          maxSchemaAgreementWaitSeconds)
  

}