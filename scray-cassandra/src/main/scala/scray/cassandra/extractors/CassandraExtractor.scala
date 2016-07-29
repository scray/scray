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
import com.datastax.driver.core.Session
import scray.querying.sync.DbSession
import scray.cassandra.sync.CassandraDbSession
import scray.cassandra.CassandraQueryableSource
import scray.cassandra.sync.OnlineBatchSyncCassandra
import scray.cassandra.sync.CassandraJobInfo
import com.twitter.util.FuturePool
import com.datastax.driver.core.{ Row => CassRow }
import scray.querying.source.store.QueryableStoreSource

/**
 * Helper class to create a configuration for a Cassandra table
 */
class CassandraExtractor[Q <: DomainQuery](session: Session, table: TableIdentifier, futurePool: FuturePool) extends StoreExtractor[CassandraQueryableSource[Q]] with LazyLogging {

  import scala.collection.convert.decorateAsScala.asScalaBufferConverter
  
  private def getTableMetaData(keyspace: String = table.dbId, tablename: String = table.tableId) =
    CassandraUtils.getTableMetadata(TableIdentifier("", keyspace, tablename), session)

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
  def getMetadata(): KeyspaceMetadata = CassandraUtils.getKeyspaceMetadata(session, table.dbId)
  
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
    ColumnConfiguration(column, indexConfig)
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
//    def createManualIndexConfiguration(column: Column, queryspaceName: String, version: Int, store: S,
//      indexes: Map[_ <: (QueryableStoreSource[_], String), _ <: (QueryableStoreSource[_], String, 
//              IndexConfig, Option[Function1[_,_]], Set[String])],
//      mappers: Map[_ <: QueryableStoreSource[_], ((_) => Row, Option[String], Option[VersioningConfiguration[_, _]])]):
//        Option[ManuallyIndexConfiguration[_, _, _, _, _]]

  
  override def createManualIndexConfiguration(column: Column, queryspaceName: String, version: Int, store: CassandraQueryableSource[Q],
      indexes: Map[_ <: (QueryableStoreSource[_ <: DomainQuery], String), _ <: (QueryableStoreSource[_ <: DomainQuery], String, 
              IndexConfig, Option[Function1[_,_]], Set[String])],
      mappers: Map[_ <: QueryableStoreSource[_], ((_) => Row, Option[String], Option[VersioningConfiguration[_, _]])]):
        Option[ManuallyIndexConfiguration[_, _, _, _, _]] = {
    def internalStores[A <: QueryableStoreSource[_]](intmappers: Map[A, 
        ((_) => Row, Option[String], Option[VersioningConfiguration[_, _]])], indexStore: CassandraQueryableSource[_]) = {
      (intmappers.get(indexStore.asInstanceOf[A]).get, intmappers.get(store.asInstanceOf[A]).get)
    }
    def internalManualIndexConfig[D <: (QueryableStoreSource[_ <: DomainQuery], String), F <: (QueryableStoreSource[_ <: DomainQuery], String, 
              IndexConfig, Option[Function1[_,_]], Set[String])](indexes2: Map[D, F] ) = {
      indexes2.get((store, column.columnName).asInstanceOf[D]).map { (index) =>
        // TODO: fix this ugly stuff (for now we leave it, as fixing this will only increase type safety)
        index._1 match {
          case indexStore: CassandraQueryableSource[u] =>
            val (indexstoreinfo, storeinfo) = internalStores(mappers, indexStore)
            val dbSystem = Some(column.table.dbSystem)
            val indexExtractor = CassandraExtractor.getExtractor(indexStore, indexstoreinfo._2, indexstoreinfo._3, dbSystem, futurePool)
            val mainDataTableTI = getTableIdentifier(store, None, dbSystem)
            def convertedSource[U <: DomainQuery](extractor: CassandraExtractor[U]): TableIdentifier = {
              extractor.getTableIdentifier(indexStore.asInstanceOf[CassandraQueryableSource[U]], indexstoreinfo._2, dbSystem)
            }
            ManuallyIndexConfiguration[DomainQuery, DomainQuery, Any, Any, DomainQuery](
              () => getTableConfigurationFunction[DomainQuery, DomainQuery, Any](mainDataTableTI, queryspaceName, version),
              () => getTableConfigurationFunction[DomainQuery, DomainQuery, Any](
                  convertedSource(indexExtractor), 
                  queryspaceName, version),
              index._4.asInstanceOf[Option[Any => DomainQuery]],
              index._5.map(Column(_, mainDataTableTI)),
              index._3
            )
          case _ => throw new RuntimeException("Using stores different from CassandraQueryableSource not supported")
        }
      }
    }
    internalManualIndexConfig(indexes)
  }
  
  private def getTableConfigurationFunction[Q <: DomainQuery, K <: DomainQuery, V](ti: TableIdentifier, space: String, version: Int): TableConfiguration[Q, K, V] = 
    Registry.getQuerySpaceTable(space, version, ti).get.asInstanceOf[TableConfiguration[Q, K, V]]
  
  private def getVersioningConfiguration[Q <: DomainQuery, K <: DomainQuery](jobName: String, rowMapper: (_) => Row): Option[(Set[Column], Set[Column], Set[Column], VersioningConfiguration[Q, K])]  = {
    val cassSession = new CassandraDbSession(session)
    // TODO: we need to include a way to define the coordinates/TableIdentifier where to search for SyncTable 
    val syncApiInstance = new OnlineBatchSyncCassandra(cassSession)
    val jobInfo = new CassandraJobInfo(jobName)
    val latestComplete = () => syncApiInstance.getBatchVersion(jobInfo)
    val runtimeVersion = () => syncApiInstance.getOnlineVersion(jobInfo)
    // if we have a runtime Version, this will be the table to read from, for now. We
    // expect that all data is aggregated with the corresponding complete version.
    // TODO: in the future we want this to be able to merge, such that it will be an
    // option to have a runtime and a batch version at once and the merge is done at
    // query time
    val tableToReadOpt: Option[TableIdentifier] = runtimeVersion().orElse(latestComplete().orElse(None))
    tableToReadOpt.map { tableToRead =>
      // use our session to fetch relevent metadata
      val tableMeta = getTableMetaData(tableToRead.dbId, tableToRead.tableId)
logger.info("########################" + tableToRead.dbId + "#" + tableToRead.tableId + "#")
      val rowKeys = getRowKeyColumnsPrivate(tableMeta)
      val clusteringKeys = getClusteringKeyColumnsPrivate(tableMeta)
      val allColumns = getColumnsPrivate(tableMeta)    
      val cassQuerySource = new CassandraQueryableSource(tableToRead,
        rowKeys, 
        clusteringKeys,
        allColumns,
        allColumns.map(col => 
          // TODO: ManualIndexConfiguration and Map of Splitter must be extracted from config
          getColumnConfiguration(cassSession, tableToRead.dbId, tableToRead.tableId, Column(col.columnName, tableToRead), None, Map())),
        session,
        new DomainToCQLQueryMapping[Q, CassandraQueryableSource[Q]](),
        futurePool,
        rowMapper.asInstanceOf[CassRow => Row])
      (rowKeys, clusteringKeys, allColumns, VersioningConfiguration[Q, K] (
        latestCompleteVersion = latestComplete,
        runtimeVersion = runtimeVersion,
        queryableStore = Some(cassQuerySource), // the versioned queryable store representation, allowing to query the store
        readableStore = Some(cassQuerySource.asInstanceOf[CassandraQueryableSource[K]]) // the versioned readablestore, used in case this is used by a HashJoinSource
      ))
    }
  }
    
  override def getTableConfiguration(rowMapper: (_) => Row, readSyncTable: Boolean, nickName: Option[String] = None): TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _] = {
    val versioningConfig = if(readSyncTable) getVersioningConfiguration[Q, Q](table.tableId , rowMapper) else None
    val rowKeys = versioningConfig.map(_._1).getOrElse(getRowKeyColumns)
    val clusterKeys = versioningConfig.map(_._2).getOrElse(getClusteringKeyColumns)
    val allColumns = versioningConfig.map(_._3).getOrElse(getColumns)
    val cassQuerySource = if(versioningConfig.isDefined) {
      None 
    } else {
      val cassSession = new CassandraDbSession(session)
      Some(new CassandraQueryableSource(table,
        rowKeys, 
        clusterKeys,
        allColumns,
        allColumns.map(col => 
          // TODO: ManualIndexConfiguration and Map of Splitter must be extracted from config
          getColumnConfiguration(cassSession, table.dbId, table.tableId, Column(col.columnName, table), None, Map())),
        session,
        new DomainToCQLQueryMapping[Q, CassandraQueryableSource[Q]](),
        futurePool,
        rowMapper.asInstanceOf[CassRow => Row])
      )
    }
    TableConfiguration[Q, Q, CassRow] (
      nickName.map(newname => table.copy(tableId = newname)).getOrElse(table),
      versioningConfig.map(_._4),
      rowKeys,
      clusterKeys,
      allColumns,
      rowMapper.asInstanceOf[CassRow => Row],
      cassQuerySource.map(_.mappingFunction.asInstanceOf[DomainQuery => Q]).getOrElse {
        versioningConfig.map(_._4.queryableStore.get.asInstanceOf[CassandraQueryableSource[Q]].mappingFunction).orNull.asInstanceOf[DomainQuery => Q]
      },
      cassQuerySource,
      cassQuerySource,
      // TODO: add materialized views
      List()
    )
  }

}

object CassandraExtractor {
  
  def getExtractor[R <: DomainQuery](store: CassandraQueryableSource[R], tableName: Option[String],
          versions: Option[VersioningConfiguration[_, _]], dbSystem: Option[String], futurePool: FuturePool):
          CassandraExtractor[R] = {
    new CassandraExtractor(store.session, 
        TableIdentifier(dbSystem.getOrElse(store.ti.dbSystem),
            store.ti.dbId, tableName.getOrElse(store.ti.tableId)),
        futurePool)
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
