package scray.hdfs.extractors


import com.twitter.util.FuturePool

import scray.hdfs.HDFSQueryableSource
import scray.querying.description.Column
import scray.querying.description.ColumnConfiguration
import scray.querying.description.ManuallyIndexConfiguration
import scray.querying.description.Row
import scray.querying.description.TableConfiguration
import scray.querying.description.TableIdentifier
import scray.querying.description.VersioningConfiguration
import scray.querying.queries.DomainQuery
import scray.querying.source.indexing.IndexConfig
import scray.querying.storeabstraction.StoreExtractor
import scray.querying.sync2.DbSession
import scray.querying.source.store.QueryableStoreSource
import org.apache.hadoop.io.Writable
import scray.hdfs.index.HDFSBlobResolver
import scray.querying.source.Splitter

/**
 * a simple extractor to be used with HDFS
 */
class HDFSExtractor[Q <: DomainQuery, T <: org.apache.hadoop.io.Text, S <: HDFSQueryableSource[Q, T]](ti: TableIdentifier, 
    directory: String, futurePool: FuturePool) extends StoreExtractor[S] {
  System.setProperty("hadoop.home.dir", "/")
  val blobResolver = new HDFSBlobResolver[org.apache.hadoop.io.Text](ti, directory)
  
  override def getColumns: Set[Column] = HDFSQueryableSource.getAllColumns(ti)
  override def getClusteringKeyColumns: Set[Column] = Set()
  override def getRowKeyColumns: Set[Column] = HDFSQueryableSource.getRowKeyColumns(ti)
  override def getValueColumns: Set[Column] = HDFSQueryableSource.getValueColumn(ti)
  override def getTableConfiguration(rowMapper: (_) => Row): TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _] = {
    val store = Some(new HDFSQueryableSource[Q, T](ti, blobResolver, futurePool))
    TableConfiguration(ti, None, getRowKeyColumns, getClusteringKeyColumns, getColumns, null, store, store)
  }
  override def getDefaultDBSystem: String = "HDFS"
  override def getTableIdentifier(store: S, tableName: Option[String], dbSystem: Option[String]): TableIdentifier = ti
  override def getColumnConfiguration(session: DbSession[_, _, _, _],
      dbName: String,
      table: String,
      column: Column,
      index: Option[ManuallyIndexConfiguration[_ <: DomainQuery, _ <: DomainQuery, _, _, _ <: DomainQuery]],
      splitters: Map[Column, Splitter[_]]): ColumnConfiguration = ColumnConfiguration(column, None)
      
  override def createManualIndexConfiguration(column: Column, queryspaceName: String, version: Int, store: S,
      indexes: Map[_ <: (QueryableStoreSource[_ <: DomainQuery], String), _ <: (QueryableStoreSource[_ <: DomainQuery], String, 
              IndexConfig, Option[Function1[_,_]], Set[String])],
      mappers: Map[_ <: QueryableStoreSource[_], ((_) => Row, Option[String], Option[VersioningConfiguration[_, _]])]):
        Option[ManuallyIndexConfiguration[_ <: DomainQuery, _ <: DomainQuery, _, _, _ <: DomainQuery]] = None
  }