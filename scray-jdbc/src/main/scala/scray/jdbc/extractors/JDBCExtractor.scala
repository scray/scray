package scray.jdbc.extractors

import scray.querying.storeabstraction.StoreExtractor
import scray.querying.queries.DomainQuery
import scray.jdbc.JDBCQueryableSource
import scray.querying.description.VersioningConfiguration
import scray.querying.source.indexing.IndexConfig
import scray.querying.description.QueryspaceConfiguration
import scray.querying.description.TableIdentifier
import scray.querying.description.TableConfiguration
import scray.querying.description.ManuallyIndexConfiguration
import scray.querying.description.ColumnConfiguration
import scray.querying.source.store.QueryableStoreSource
import scray.querying.sync.DbSession
import scray.querying.description.Row
import scray.querying.source.Splitter
import scray.querying.Registry
import scray.querying.description.Column
import java.sql.Connection
import java.sql.ResultSet
import scala.collection.mutable.ListBuffer
import scala.annotation.tailrec


/**
 * This is only a first version that is able to 
 */
class JDBCExtractors[Q <: DomainQuery](connection: Connection, ti: TableIdentifier, sqlDialect: ScraySQLDialect) extends StoreExtractor[JDBCQueryableSource[Q]] {

  private val queryMapping = new DomainToSQLQueryMapping
  
  // cache metadata connection for later use
  private val dbmsMetadata  = connection.getMetaData()
  
  /**
   * transforms a metadata query for columns into a internal column representation
   */
  @tailrec private def scanColumns(resSet: ResultSet, columns: ListBuffer[JDBCColumn]): Seq[JDBCColumn] = {
    if(resSet.next()) {
      val newColumn = JDBCColumn(
        resSet.getString("COLUMN_NAME"), 
        ti,
        resSet.getInt("DATA_TYPE")
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
      val rowKeyCols = scanColumns(rs, new ListBuffer[JDBCColumn]())
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
    TableConfiguration(ti, getRowKeyColumns, getClusteringKeyColumns, getColumns

  /**
   * returns a query mapping
   */
  def getQueryMapping(store: S, tableName: Option[String]): DomainQuery => String = queryMapping.getQueryMapping(store, tableName, sqlDialect)

  /**
   * DB-System is fixed
   */
  def getDefaultDBSystem: String = sqlDialect.name
  
  /**
   * returns a table identifier for this store
   */
  def getTableIdentifier(store: S, tableName: Option[String], dbSystem: Option[String]): TableIdentifier = ti
    
  /**
   * returns the column configuration for a column
   */
//  def getColumnConfiguration(store: S, 
//      column: Column,
//      querySpace: QueryspaceConfiguration,
//      index: Option[ManuallyIndexConfiguration[_, _, _, _, _]],
//      splitters: Map[Column, Splitter[_]]): ColumnConfiguration
  
  def getColumnConfiguration(session: DbSession[_, _, _],
      dbName: String,
      table: String,
      column: Column,
      index: Option[ManuallyIndexConfiguration[_, _, _, _, _]],
      splitters: Map[Column, Splitter[_]]): ColumnConfiguration = {
    ColumnConfiguration(column, )
  }
      
  /**
   * returns all column configurations
   */
  def getColumnConfigurations(session: DbSession[_, _, _],
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
  def createManualIndexConfiguration(column: Column, queryspaceName: String, version: Int, store: S,
      indexes: Map[_ <: (QueryableStoreSource[_ <: DomainQuery], String), _ <: (QueryableStoreSource[_ <: DomainQuery], String, 
              IndexConfig, Option[Function1[_,_]], Set[String])],
      mappers: Map[_ <: QueryableStoreSource[_], ((_) => Row, Option[String], Option[VersioningConfiguration[_, _]])]):
        Option[ManuallyIndexConfiguration[_, _, _, _, _]] = None
  
  private def getTableConfigurationFunction[Q <: DomainQuery, K <: DomainQuery, V](ti: TableIdentifier, space: String, version: Int): TableConfiguration[Q, K, V] = 
    Registry.getQuerySpaceTable(space, version, ti).get.asInstanceOf[TableConfiguration[Q, K, V]]
}

