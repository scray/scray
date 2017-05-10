package scray.jdbc.extractors

import java.sql.Connection
import java.sql.ResultSet

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

import scray.jdbc.JDBCQueryableSource
import scray.querying.Registry
import scray.querying.description.Column
import scray.querying.description.ColumnConfiguration
import scray.querying.description.ManuallyIndexConfiguration
import scray.querying.description.QueryspaceConfiguration
import scray.querying.description.Row
import scray.querying.description.TableConfiguration
import scray.querying.description.TableIdentifier
import scray.querying.description.VersioningConfiguration
import scray.querying.queries.DomainQuery
import scray.querying.source.Splitter
import scray.querying.source.indexing.IndexConfig
import scray.querying.storeabstraction.StoreExtractor
import scray.querying.sync.DbSession
import com.zaxxer.hikari.HikariDataSource
import scray.querying.source.store.QueryableStoreSource
import com.twitter.util.FuturePool
import scray.jdbc.sync.JDBCDbSession
import scray.querying.description.AutoIndexConfiguration
import scray.querying.description.IndexConfiguration
import java.sql.Types
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.querying.description.internal.Domain
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.HashTable
import scala.collection.mutable.HashMap
import java.sql.DatabaseMetaData


/**
 * This is only a first version that is able to 
 */
class JDBCExtractors[Q <: DomainQuery, S <: JDBCQueryableSource[Q]](
    ti: TableIdentifier, hikari: HikariDataSource, metadataConnection: Connection,
    sqlDialect: ScraySQLDialect, futurePool: FuturePool) extends StoreExtractor[JDBCQueryableSource[Q]] with LazyLogging {

  private val queryMapping = new DomainToSQLQueryMapping[Q, S]
  
  // cache metadata connection for later use
  private val dbmsMetadata  = JDBCExtractors.fetchOrGetDatabaseMetadata(ti.dbSystem, ti.dbId, metadataConnection)
  
  
  /**
   * transforms a metadata query for columns into a internal column representation
   */
  @tailrec private def scanColumns(resSet: ResultSet, columns: ListBuffer[JDBCColumn], typ: Boolean = true): Seq[JDBCColumn] = {
    if(resSet.next()) {
      val newColumn = JDBCColumn(
        resSet.getString("COLUMN_NAME"), 
        ti,
        if(typ) Some(resSet.getInt("DATA_TYPE")) else None
        )
      scanColumns(resSet, columns += newColumn)
    } else {
      columns.toSeq
    }
  }
  
  // cache all column results for later use
  val columnMetadata = {
    val rs = dbmsMetadata.getColumns(null, ti.dbId, ti.tableId, null)
    try {
      scanColumns(rs, new ListBuffer[JDBCColumn]())
    } finally {
      rs.close
    }
  }
  
  // transformation to Scray column format
  val columns = columnMetadata.map(col => Column(col.name, col.ti)).toSet
  
  // cache primary key columns
  val rowKeyColumns = {
    val rs = dbmsMetadata.getPrimaryKeys(null, ti.dbId, ti.tableId)
    try {
      val rowKeyCols = scanColumns(rs, new ListBuffer[JDBCColumn](), false)
      if(rowKeyCols.isEmpty) {
        // primary key are all columns if there is no primary key defined
        columns
      } else {
        // don't need to check ti here, as it will always have the same content 
        columns.filter(col => rowKeyCols.find(rcol => rcol.name == col.columnName).isDefined)
      }
    } finally {
      rs.close
    }
  }

  
  
  // at present we can only use non-combined indexes
  val indexInformations = JDBCExtractors.fetchOrGetIndexInformation(dbmsMetadata, ti)

  
  // assume that value columns are all columns that are not indexed
  
  
  /**
   * returns a list of columns for this specific store; implementors must override this
   */
  def getColumns: Set[Column] = columns
  
  /**
   * returns list of clustering key columns for this specific store; implementors must override this
   */
  def getClusteringKeyColumns: Set[Column] = columns
  
  /**
   * returns list of row key columns for this specific store; implementors must override this
   */
  def getRowKeyColumns: Set[Column] = rowKeyColumns

  /**
   * returns a list of value key columns for this specific store; implementors must override this
   */
  def getValueColumns: Set[Column] = columns // for JDBC all columns can be value columns; btw. seems not to be used

  /**
   * returns the table configuration for this specific store; implementors must override this
   */
  def getTableConfiguration(rowMapper: (_) => Row): TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _] = {
    val jdbcSession = new JDBCDbSession(hikari, metadataConnection, sqlDialect)
    val jdbcSource = Some(new JDBCQueryableSource(ti,
        getRowKeyColumns, 
        getClusteringKeyColumns,
        getColumns,
        getColumns.map(col => 
          // TODO: ManualIndexConfiguration and Map of Splitter must be extracted from config
          getColumnConfiguration(jdbcSession, ti.dbId, ti.tableId, Column(col.columnName, ti), None, Map())),
        hikari,
        new DomainToSQLQueryMapping[Q, JDBCQueryableSource[Q]](),
        futurePool,
        rowMapper.asInstanceOf[ResultSet => Row],
        sqlDialect)
      )  
    TableConfiguration(
        ti, 
        None, 
        getRowKeyColumns, 
        getClusteringKeyColumns, 
        getColumns, 
        rowMapper,
        jdbcSource,
        jdbcSource
     )
  }
     
  /**
   * returns a query mapping
   */
  def getQueryMapping(store: S, tableName: Option[String]): DomainQuery => (String, Int, List[Domain[_]]) = 
    queryMapping.getQueryMapping(store, tableName, sqlDialect)

  /**
   * DB-System is fixed
   */
  def getDefaultDBSystem: String = sqlDialect.name
  
  /**
   * returns a table identifier for this store
   */
  override def getTableIdentifier(store: JDBCQueryableSource[Q], tableName: Option[String], dbSystem: Option[String]): TableIdentifier = 
    tableName.map(name => TableIdentifier(dbSystem.getOrElse(store.ti.dbSystem), store.ti.dbId, name)).
      getOrElse(dbSystem.map(system => TableIdentifier(dbSystem.getOrElse(store.ti.dbSystem), store.ti.dbId, store.ti.tableId)).getOrElse(store.ti))
  
      
      
  private def getAutoIndexConfiguration(colName: String): Option[IndexConfiguration] = 
    indexInformations.get(colName).map { index =>
      IndexConfiguration (
        isAutoIndexed = true,
        isManuallyIndexed = None, 
        isSorted = index.ordered, 
        isGrouped = index.ordered,
        isRangeQueryable = index.ordered, // if this index can be range queried
        autoIndexConfiguration = if(index.ordered) {
          Some(AutoIndexConfiguration(true, false, true, None)) 
        } else { None }
      )
    }

  /**
   * returns the column configuration for a column
   */
  def getColumnConfiguration(session: DbSession[_, _, _],
      dbName: String,
      table: String,
      column: Column,
      index: Option[ManuallyIndexConfiguration[_, _, _, _, _]],
      splitters: Map[Column, Splitter[_]]): ColumnConfiguration = {
    ColumnConfiguration(column, getAutoIndexConfiguration(column.columnName))
  }
      
  /**
   * returns all column configurations
   */
  override def getColumnConfigurations(session: DbSession[_, _, _],
      dbName: String,
      table: String,
      querySpace: QueryspaceConfiguration, 
      indexes: Map[String, ManuallyIndexConfiguration[_, _, _, _, _]],
      splitters: Map[Column, Splitter[_]]): Set[ColumnConfiguration] = {
    getColumns.map(col => getColumnConfiguration(session, dbName, table, col, indexes.get(col.columnName), splitters))
  }
  
  /**
   * return a manual index configuration for a column
   */
  override def createManualIndexConfiguration(column: Column,queryspaceName: String,version: Int,store: JDBCQueryableSource[Q], 
      indexes: Map[_ <: (QueryableStoreSource[_ <: DomainQuery], String), _ <: (QueryableStoreSource[_ <: DomainQuery], String, 
          IndexConfig, Option[Function1[_, _]], Set[String])],
      mappers: Map[_ <: QueryableStoreSource[_], ((_) => Row, Option[String], Option[VersioningConfiguration[_, _]])]): 
      Option[ManuallyIndexConfiguration[_, _, _, _, _]] = None

  private def getTableConfigurationFunction[Q <: DomainQuery, K <: DomainQuery, V](ti: TableIdentifier, space: String, version: Int): TableConfiguration[Q, K, V] = 
    Registry.getQuerySpaceTable(space, version, ti).get.asInstanceOf[TableConfiguration[Q, K, V]]
  
  // TODO: versioning must still be implemented
//  private def getVersioningConfiguration[Q <: DomainQuery, K <: DomainQuery](jobName: String, rowMapper: (_) => Row): Option[VersioningConfiguration[Q, K]]  = {
//    val cassSession = new CassandraDbSession(session)
//    val syncApiInstance = new OnlineBatchSyncCassandra(cassSession)
//    val jobInfo = new CassandraJobInfo(jobName)
//    val latestComplete = () => None // syncApiInstance.getBatchVersion(jobInfo) // FIXME 
//    val runtimeVersion = () => None // syncApiInstance.getOnlineVersion(jobInfo)
//    // if we have a runtime Version, this will be the table to read from, for now. We
//    // expect that all data is aggregated with the corresponding complete version.
//    // TODO: in the future we want this to be able to merge, such that it will be an
//    // option to have a runtime and a batch version at once and the merge is done at
//    // query time
//    val tableToReadOpt: Option[TableIdentifier] = runtimeVersion().orElse(latestComplete().orElse(None))
//    tableToReadOpt.map { tableToRead =>
//      // use our session to fetch relevent metadata
//      val tableMeta = getTableMetaData(tableToRead.dbId, tableToRead.tableId)
//      
//      val allColumns = getColumnsPrivate(tableMeta)    
//      val cassQuerySource = new CassandraQueryableSource(tableToRead,
//        getRowKeyColumnsPrivate(tableMeta), 
//        getClusteringKeyColumnsPrivate(tableMeta),
//        allColumns,
//        allColumns.map(col => 
//          // TODO: ManualIndexConfiguration and Map of Splitter must be extracted from config
//          getColumnConfiguration(cassSession, tableToRead.dbId, tableToRead.tableId, Column(col.columnName, tableToRead), None, Map())),
//        session,
//        new DomainToCQLQueryMapping[Q, CassandraQueryableSource[Q]](),
//        futurePool,
//        rowMapper.asInstanceOf[CassRow => Row])
//      VersioningConfiguration[Q, K] (
//        latestCompleteVersion = latestComplete,
//        runtimeVersion = runtimeVersion,
//        queryableStore = Some(cassQuerySource), // the versioned queryable store representation, allowing to query the store
//        readableStore = Some(cassQuerySource.asInstanceOf[CassandraQueryableSource[K]]) // the versioned readablestore, used in case this is used by a HashJoinSource
//      )
//    }
//  }

}

/**
 * since fetching database meta-information seems to be expensive for most relational
 * DBMS, we fetch and cache
 */
object JDBCExtractors extends LazyLogging {
  
  case class TemporaryIndexInfo(column: String, ordered: Boolean, count: Int, lastOrdinal: Short)
  
  private val lock = new ReentrantLock()
  
  private val dbmsHash = new HashMap[(String, String), DatabaseMetaData]()
  private val indexHash = new HashMap[TableIdentifier, Map[String, TemporaryIndexInfo]]()
  
  def fetchOrGetDatabaseMetadata(dbms: String, dbId: String, connection: Connection): DatabaseMetaData = {
    lock.lock()
    try {
      dbmsHash.get((dbms, dbId)).getOrElse { 
        val meta = connection.getMetaData
        dbmsHash.put((dbms, dbId), meta)
        meta
      }
    } finally {
      lock.unlock()
    }
  }
  
  def fetchOrGetIndexInformation(dbMeta: DatabaseMetaData, ti: TableIdentifier): Map[String, TemporaryIndexInfo] = {
    def getIndexInfos(rs: ResultSet, indexes: ListBuffer[JDBCExtractors.TemporaryIndexInfo], 
        lastIndexRow: Option[JDBCExtractors.TemporaryIndexInfo]): List[JDBCExtractors.TemporaryIndexInfo] = {
      if(rs.next) {
        Option(rs.getString("INDEX_NAME")).map { _ =>
          val newIndex = JDBCExtractors.TemporaryIndexInfo(rs.getString("COLUMN_NAME"), 
              Option(rs.getString("ASC_OR_DESC")).map(_ => true).getOrElse(false), 
              1, 
              rs.getShort("ORDINAL_POSITION"))
          lastIndexRow.map { index =>
            if(rs.getShort("ORDINAL_POSITION") <= index.lastOrdinal) {
              // found next index, add the last if there is only one entry
              if(index.count == 1) {
                getIndexInfos(rs, indexes += index, Some(newIndex))
              } else {
                getIndexInfos(rs, indexes, Some(newIndex))
              }
            } else {
              // found another row for the index at hand => do not use it, but increase count to at least 2
              getIndexInfos(rs, indexes, Some(index.copy(count = 2)))
            }
          }.getOrElse(getIndexInfos(rs, indexes, Some(newIndex)))
        }.getOrElse(getIndexInfos(rs, indexes, lastIndexRow))
      } else {
        lastIndexRow.map { index =>
          if(index.count == 1) {
            (indexes += index).toList
          } else {
            indexes.toList
          }
        }.getOrElse(indexes.toList)
      }
    }

    lock.lock()
    try {
      indexHash.get(ti).getOrElse {
        val rs = dbMeta.getIndexInfo(null, ti.dbId, ti.tableId, false, false)
        try {
          val iInfo = getIndexInfos(rs, new ListBuffer[JDBCExtractors.TemporaryIndexInfo], None)
          iInfo.foreach(index => logger.debug(s"Found index for ${ti.dbSystem}.${ti.dbId}.${ti.tableId}.${index.column}"))
          val result = iInfo.map(index => index.column -> index).toMap
          indexHash.put(ti, result)
          result
        } finally {
          rs.close
        }
      }
    } finally {
      lock.unlock()
    }
  }
  
}