package scray.hdfs.automation

import scray.querying.storeabstraction.StoreGenerators
import scray.querying.description.VersioningConfiguration
import scray.querying.queries.DomainQuery
import scray.querying.description.TableIdentifier
import scray.querying.source.store.QueryableStoreSource
import scray.querying.description.Row
import scray.querying.storeabstraction.StoreExtractor
import com.twitter.util.FuturePool
import org.apache.hadoop.io.Writable
import scray.hdfs.HDFSQueryableSource
import scray.hdfs.index.HDFSBlobResolver
import scray.hdfs.extractors.HDFSExtractor

class HDFSStoreGenerators[T <: org.apache.hadoop.io.Text](directory: String, futurePool: FuturePool) extends StoreGenerators {
  
  def createRowStore[Q <: DomainQuery](table: TableIdentifier): Option[(QueryableStoreSource[Q], ((_) => Row, Option[String], Option[VersioningConfiguration[_, _]]))] = {
    val extractor = new HDFSExtractor[Q, T, HDFSQueryableSource[Q, T]](table, directory, futurePool)
    val source = new HDFSQueryableSource[Q, T](table, extractor.blobResolver, futurePool)
    val rowMapper: (_) => Row = (row: Row) => row
    Some((source, (rowMapper, None, None)))
  }
  
  override def getExtractor[Q <: DomainQuery, S <: QueryableStoreSource[Q]](
      store: S, tableName: Option[String], versions: Option[VersioningConfiguration[_, _]], 
      dbSystem: Option[String], futurePool: FuturePool): StoreExtractor[S] = {
    new HDFSExtractor[Q, T, HDFSQueryableSource[Q, T]](store.asInstanceOf[HDFSQueryableSource[Q, T]].ti, directory, futurePool).asInstanceOf[StoreExtractor[S]]
  }
}