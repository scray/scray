// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package scray.cassandra.extractors

import com.datastax.driver.core.{ KeyspaceMetadata, Metadata, TableMetadata }
import com.twitter.storehaus.cassandra.cql.{ AbstractCQLCassandraStore, CQLCassandraCollectionStore, CQLCassandraRowStore, CQLCassandraStoreTupleValues }
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration.StoreColumnFamily
import com.typesafe.scalalogging.slf4j.LazyLogging
import java.util.regex.Pattern
import org.yaml.snakeyaml.Yaml
import scray.cassandra.util.CassandraUtils
import scray.querying.Registry
import scray.querying.description.{
  AutoIndexConfiguration,
  Column,
  ManuallyIndexConfiguration,
  ColumnConfiguration,
  QueryspaceConfiguration,
  IndexConfiguration,
  TableIdentifier, 
  TableConfiguration,
  Row,
  VersioningConfiguration
}
import scray.querying.description.internal.SingleValueDomain
import scray.querying.queries.DomainQuery
import scray.querying.source.Splitter
import scray.querying.source.indexing.IndexConfig
import scray.querying.storeabstraction.StoreExtractor
import com.twitter.storehaus.QueryableStore
import com.datastax.driver.core.Session
import scray.querying.sync.DbSession
import scray.cassandra.sync.CassandraDbSession
import scray.cassandra.CassandraQueryableSource
import scray.cassandra.sync.OnlineBatchSyncCassandra
import com.twitter.storehaus.ReadableStore
import scray.cassandra.sync.CassandraJobInfo

/**
 * Helper class to create a configuration for a Cassandra table
 */
class CassandraExtractor[Q <: DomainQuery](session: Session, table: TableIdentifier) extends StoreExtractor[CassandraQueryableSource[Q]] with LazyLogging {

  import scala.collection.convert.decorateAsScala.asScalaBufferConverter
  
  private def getTableMetaData(keyspace: String = table.dbId, tablename: String = table.tableId) = {
    session.getCluster.getMetadata.getKeyspace(keyspace).getTable(tablename)
  }

  private def getColumnsPrivate(tm: TableMetadata): Set[Column] = tm.getColumns.asScala.map(col => Column(col.getName, table)).toSet
  private def getClusteringKeyColumnsPrivate(tm: TableMetadata): Set[Column] = 
    tm.getClusteringColumns.asScala.map(col => Column(col.getName, table)).toSet
  private def getRowKeyColumnsPrivate(tm: TableMetadata): Set[Column] =
    tm.getPartitionKey.asScala.map(col => Column(col.getName, table)).toSet    
  
  /**
   * returns a list of columns for this specific store; implementors must override this
   */
  override def getColumns: Set[Column] = getColumnsPrivate(getTableMetaData())
  
  /**
   * returns list of clustering key columns for this specific store; implementors must override this
   */
  override def getClusteringKeyColumns: Set[Column] = getClusteringKeyColumnsPrivate(getTableMetaData())

  /**
   * returns list of row key columns for this specific store; implementors must override this
   */
  override def getRowKeyColumns: Set[Column] = getRowKeyColumnsPrivate(getTableMetaData()) 

  /**
   * returns a list of value key columns for this specific store; implementors must override this
   */
  def getValueColumns: Set[Column] =
    getColumns -- getClusteringKeyColumns -- getRowKeyColumns

  /**
   * returns the table configuration for this specific store; implementors must override this
   */
  def getTableConfiguration(rowMapper: (_) => Row): TableConfiguration[_, _, _]

  /**
   * returns a generic Cassandra-store query mapping
   */
  override def getQueryMapping(store: CassandraQueryableSource[Q], tableName: Option[String]): DomainQuery => String =
    new DomainToCQLQueryMapping[Q, CassandraQueryableSource[Q]]().getQueryMapping(store, tableName)

  /**
   * returns the name of the dbSystem
   */
  def getDBSystem: String = table.dbSystem
    
  /**
   * DB-System is fixed
   */
  override def getDefaultDBSystem: String = CassandraExtractor.DB_ID
  
  /**
   * returns a table identifier for this cassandra store
   */
  override def getTableIdentifier(store: CassandraQueryableSource[Q], tableName: Option[String], dbSystem: Option[String]): TableIdentifier =
    tableName.map(TableIdentifier(dbSystem.getOrElse(getDefaultDBSystem), table.dbId, _)).
      getOrElse(TableIdentifier(dbSystem.getOrElse(getDefaultDBSystem), table.dbId, table.tableId))
    

  /**
   * returns metadata information from Cassandra
   */
  def getMetadata(cf: StoreColumnFamily): KeyspaceMetadata = CassandraUtils.getKeyspaceMetadata(cf)
  
  /**
   * return whether and maybe how the given column is auto-indexed by Cassandra-Lucene-Plugin 
   */
  private def getColumnCassandraLuceneIndexed(tmOpt: Option[TableMetadata], column: Column, 
                                              splitters: Map[Column, Splitter[_]]): Option[AutoIndexConfiguration[_]] = {
    val cmOpt = tmOpt.flatMap { tm => Option(tm.getColumn(Metadata.quote(CassandraExtractor.LUCENE_COLUMN_NAME))) }
    val schemaOpt = cmOpt.flatMap (cm => Option(cm.getIndex).map(_.getOption(CassandraExtractor.LUCENE_INDEX_SCHEMA_OPTION_NAME)))
    schemaOpt.flatMap { schema =>
      logger.trace(s"Lucene index schema is: $schema")
      val outerMatcher = CassandraExtractor.outerPattern.matcher(schema) 
      if(outerMatcher.matches()) {
        val fieldString = outerMatcher.group(1)
        if(CassandraExtractor.innerPattern.split(fieldString, -1).find { _.trim() == column.columnName }.isDefined) {
          cmOpt.get.getType
          if(splitters.get(column).isDefined) {
            logger.debug(s"Found Lucene-indexed column ${column.columnName} for table ${tmOpt.get.getName} with splitting option")
          } else {
            logger.debug(s"Found Lucene-indexed column ${column.columnName} for table ${tmOpt.get.getName}")
          }
          Some(AutoIndexConfiguration[Any](isRangeIndex = true, isFullTextIndex = true, isSorted = true,
                  rangePartioned = splitters.get(column).map(_.splitter).asInstanceOf[Option[((Any, Any), Boolean) => Iterator[(Any, Any)]]]))
        } else {
          None
        }
      } else {
        None
      }
    }
  }
  
  /**
   * checks that a column has been indexed by Cassandra itself, so no manual indexing
   * if the table has not yet been created (the version must still be computed) we assume no indexing 
   */
  def checkColumnCassandraAutoIndexed(session: DbSession[_ ,_ ,_], keyspace: String, tableName: String, column: Column, splitters: Map[Column, Splitter[_]]): (Boolean, Option[AutoIndexConfiguration[_]]) = {
    val metadata = session match {
      case cassSession: CassandraDbSession =>
        Option(CassandraUtils.getKeyspaceMetadata(cassSession.cassandraSession, keyspace))
      case _ => None
    }
    val tm = metadata.flatMap(ksmeta => Option(CassandraUtils.getTableMetadata(tableName, ksmeta)))
    val autoIndex = metadata.flatMap{_ => 
      val cm = tm.map(_.getColumn(Metadata.quote(column.columnName)))
      cm.flatMap(colmeta => Option(colmeta.getIndex()))}.isDefined
    val autoIndexConfig = getColumnCassandraLuceneIndexed(tm, column, splitters)
    if(autoIndexConfig.isDefined) {
      (true, autoIndexConfig)
    } else {
      (autoIndex, None)    
    }
  }

  /**
   * returns the column configuration for a Cassandra column
   */
  override def getColumnConfiguration(session: DbSession[_, _, _],
      dbName: String,
      table: String,
      column: Column,
      querySpace: QueryspaceConfiguration,
      index: Option[ManuallyIndexConfiguration[_, _, _, _, _]],
      splitters: Map[Column, Splitter[_]]): ColumnConfiguration = {
    val indexConfig = index match {
      case None =>
        val autoIndex = checkColumnCassandraAutoIndexed(session, dbName, table, column, splitters)
        if(autoIndex._1) {
          Some(IndexConfiguration(true, None, autoIndex._2.map(_.isRangeIndex).getOrElse(false), 
                                  false, autoIndex._2.map(_.isRangeIndex).getOrElse(false), autoIndex._2)) 
        } else { None }
      case Some(idx) => Some(IndexConfiguration(true, Some(idx), true, true, true, None)) 
    }
    ColumnConfiguration(column, querySpace, indexConfig)
  }

  /**
   * returns the column configuration for a Cassandra column
   */
//  def getColumnConfiguration(store: S, 
//      column: Column,
//      querySpace: QueryspaceConfiguration,
//      index: Option[ManuallyIndexConfiguration[_, _, _, _, _]],
//      splitters: Map[Column, Splitter[_]]): ColumnConfiguration = {
//    val indexConfig = index match {
//      case None =>
//        val autoIndex = checkColumnCassandraAutoIndexed(store, column, splitters)
//        if(autoIndex._1) {
//          Some(IndexConfiguration(true, None, autoIndex._2.map(_.isRangeIndex).getOrElse(false), 
//                                  false, autoIndex._2.map(_.isRangeIndex).getOrElse(false), autoIndex._2)) 
//        } else { None }
//      case Some(idx) => Some(IndexConfiguration(true, Some(idx), true, true, true, None)) 
//    }
//    ColumnConfiguration(column, querySpace, indexConfig)
//  }
  
  /**
   * return a manual index configuration for a column
   */
  override def createManualIndexConfiguration(column: Column, queryspaceName: String, version: Int, store: CassandraQueryableSource[Q],
      indexes: Map[_ <: (CassandraQueryableSource[_ <: DomainQuery], String), _ <: (CassandraQueryableSource[_ <: DomainQuery], String, 
              IndexConfig, Option[Function1[_,_]], Set[String])],
      mappers: Map[_ <: CassandraQueryableSource[_], ((_) => Row, Option[String], Option[VersioningConfiguration[_, _]])]):
        Option[ManuallyIndexConfiguration[_, _, _, _, _]] = {
    def internalStores[A <: CassandraQueryableSource[_]](intmappers: Map[A, 
        ((_) => Row, Option[String], Option[VersioningConfiguration[_, _]])], indexStore: CassandraQueryableSource[_]) = {
      (intmappers.get(indexStore.asInstanceOf[A]).get, intmappers.get(store.asInstanceOf[A]).get)
    }
    def internalManualIndexConfig[D <: (CassandraQueryableSource[_ <: DomainQuery], String), F <: (CassandraQueryableSource[_ <: DomainQuery], String, 
              IndexConfig, Option[Function1[_,_]], Set[String])](indexes2: Map[D, F] ) = {
      indexes2.get((store, column.columnName).asInstanceOf[D]).map { (index) =>
        // TODO: fix this ugly stuff (for now we leave it, as fixing this will only increase type safety)
        val indexStore = index._1
        val (indexstoreinfo, storeinfo) = internalStores(mappers, indexStore)
        val dbSystem = Some(column.table.dbSystem)
        val indexExtractor = CassandraExtractor.getExtractor2(indexStore, indexstoreinfo._2, indexstoreinfo._3, dbSystem)
        val mainDataTableTI = getTableIdentifier(store, None, dbSystem)
        ManuallyIndexConfiguration[DomainQuery, DomainQuery, Any, Any, DomainQuery](
          () => getTableConfigurationFunction[DomainQuery, DomainQuery, Any](mainDataTableTI, queryspaceName, version),
          () => getTableConfigurationFunction[DomainQuery, DomainQuery, Any](
              indexExtractor.getTableIdentifier(indexStore.asInstanceOf[CassandraQueryableSource[Nothing]], indexstoreinfo._2, dbSystem), 
              queryspaceName, version),
          index._4.asInstanceOf[Option[Any => DomainQuery]],
          index._5.map(Column(_, mainDataTableTI)),
          index._3
        )
      }
    }
    internalManualIndexConfig(indexes)
  }
  
  private def getTableConfigurationFunction[Q <: DomainQuery, K <: DomainQuery, V](ti: TableIdentifier, space: String, version: Int): TableConfiguration[Q, K, V] = 
    Registry.getQuerySpaceTable(space, version, ti).get.asInstanceOf[TableConfiguration[Q, K, V]]
  
  private def getVersioningConfiguration[Q <: DomainQuery, K <: DomainQuery](jobName: String):VersioningConfiguration[Q, K]  = {
    val cassSession = new CassandraDbSession(session)
    val syncApiInstance = new OnlineBatchSyncCassandra(cassSession)
    val jobInfo = new CassandraJobInfo(jobName)
    val latestComplete = () => Some(syncApiInstance.getBatchVersion(jobInfo))
    val runtimeVersion = () => Some(syncApiInstance.getOnlineVersion(jobInfo))
    // if we have a runtime Version, this will be the table to read from, for now. We
    // expect that all data is aggregated with the corresponding complete version.
    // TODO: in the future we want this to be able to merge, such that it will be an
    // option to have a runtime and a batch version at once and the merge is done at
    // query time
    val tableToRead: TableIdentifier = runtimeVersion().getOrElse(latestComplete())
    // use our session to fetch relevent metadata
    val tableMeta = getTableMetaData(tableToRead.dbId, tableToRead.tableId)
    
    val cassQuerySource = new CassandraQueryableSource(tableToRead,
    rowKeyColumns: Set[Column], 
    clusteringKeyColumns: Set[Column],
    allColumns: Set[Column],
    columnConfigs: Set[ColumnConfiguration],
    val session: Session,
    queryMapper: DomainToCQLQueryMapping[Q, CassandraQueryableSource[Q]],
    futurePool: FuturePool,
    rowMapper: CassRow => Row)
          getRowKeyColumnsPrivate(tableMeta),
          getClusteringKeyColumnsPrivate(tableMeta),
          getColumnsPrivate(tableMeta)
        )
    VersioningConfiguration[Q, K] (
      latestCompleteVersion = latestComplete,
      runtimeVersion = runtimeVersion,
      queryableStore = Some(cassQuerySource), // the versioned queryable store representation, allowing to query the store
      readableStore = Some(cassQuerySource) // the versioned readablestore, used in case this is used by a HashJoinSource
    )
  }
  
  
  override def getTableConfiguration(rowMapper: (_) => Row): TableConfiguration[_  <: DomainQuery, _ <: DomainQuery, _] = {
    TableConfiguration (
      table,
      versions.asInstanceOf[Option[scray.querying.description.VersioningConfiguration[Any,Any]]],
      getRowKeyColumns,
      getClusteringKeyColumns,
      getColumns,
      rowMapper.asInstanceOf[(Any) => Row],
      getQueryMapping(store, tableName),
      versions match {
        case Some(_) => None 
        case None => Some(() => store.asInstanceOf[QueryableStore[Any, Any]])
      },
      versions match {
        case Some(_) => None 
        case None => Some(() => store.asInstanceOf[ReadableStore[Any, Any]])
      },
      List()
    )
  }

}

object CassandraExtractor {
  
  def getExtractor2[Q <: DomainQuery](store: CassandraQueryableSource[Q], tableName: Option[String],
          versions: Option[VersioningConfiguration[_, _]], dbSystem: Option[String]) = {
    new CassandraExtractor(store.session, TableIdentifier(dbSystem.getOrElse(store.ti.dbSystem), store.ti.dbId, tableName.getOrElse(store.ti.tableId)))
  }
  
//  /**
//   * returns a Cassandra information extractor for a given Cassandra-Storehaus wrapper
//   */
//  def getExtractor[S <: AbstractCQLCassandraStore[_, _]](store: S, tableName: Option[String],
//          versions: Option[VersioningConfiguration[_, _]], dbSystem: Option[String]): CassandraExtractor[S] = {
//    store match { 
//      case collStore: CQLCassandraCollectionStore[_, _, _, _, _, _] => 
//        new CQLCollectionStoreExtractor(collStore, tableName, dbSystem, versions).asInstanceOf[CassandraExtractor[S]]
//      case tupleStore: CQLCassandraStoreTupleValues[_, _, _, _] =>
//        new CQLStoreTupleValuesExtractor(tupleStore, tableName, dbSystem, versions).asInstanceOf[CassandraExtractor[S]]
//      case rowStore: CQLCassandraRowStore[_] =>
//        new CQLRowStoreExtractor(rowStore, tableName, dbSystem, versions).asInstanceOf[CassandraExtractor[S]]
//    }
//  }
  
  val DB_ID: String = "cassandra"
  val LUCENE_COLUMN_NAME: String = "lucene"
  val LUCENE_INDEX_SCHEMA_OPTION_NAME: String = "schema"

  lazy val outerPattern = Pattern.compile("^\\s*\\{\\s*fields\\s*:\\s*\\{(.*)\\s*}\\s*\\}\\s*$", Pattern.DOTALL)
  lazy val innerPattern = Pattern.compile("\\s*:\\s*\\{.*?\\}\\s*,?\\s*", Pattern.DOTALL)
}
