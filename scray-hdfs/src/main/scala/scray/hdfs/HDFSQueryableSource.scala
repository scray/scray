package scray.hdfs

import scray.querying.queries.DomainQuery
import scray.querying.description.TableIdentifier
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.twitter.util.FuturePool
import scray.querying.source.store.QueryableStoreSource
import scray.querying.description.Column
import HDFSQueryableSource._
import scray.querying.queries.KeyedQuery
import com.twitter.util.Future
import scray.querying.description.Row
import scray.hdfs.index.HDFSBlobResolver
import org.apache.hadoop.io.Writable
import scray.querying.description.SimpleRow
import scala.collection.mutable.ArrayBuffer
import scray.querying.description.RowColumn
import scray.querying.description.ArrayByteColumn
import scray.querying.description.internal.SingleValueDomain
import com.twitter.concurrent.Spool
import scray.querying.description.ColumnFactory

/**
 * Some HDFS queryable source to query Blobs from HDFS
 */
class HDFSQueryableSource[Q <: DomainQuery, T <: Writable](
    val ti: TableIdentifier,
    val resolver: HDFSBlobResolver[T],
    futurePool: FuturePool) extends QueryableStoreSource[Q](ti, getRowKeyColumns(ti), Set(), getAllColumns(ti), false) 
    with LazyLogging {
  
  val rowColumn = Column(rowKeyColumnName, ti)
  val valueColumn = Column(valueColumnName, ti)
  
  override def request(query: Q): scray.querying.source.LazyDataFuture = {
    requestIterator(query).map { it =>
      it.hasNext match {
        case true => it.next() *:: Future.value(Spool.empty[Row])
        case false => Spool.empty[Row]
      }
    }
  }
  override def requestIterator(query: Q): Future[Iterator[Row]] = {
    futurePool {
      val key = query.domains.find(domain => domain.column == rowColumn).flatMap {
        case single: SingleValueDomain[_] => Some(single.value)
        case _ => None
      }
      key.map { mkey =>
        val value = resolver.getBlob(HDFSBlobResolver.transformHadoopTypes(mkey).asInstanceOf[T])
        new OneHDFSBlobIterator(rowColumn, Some(mkey), valueColumn, value)
      }.getOrElse {
        new OneHDFSBlobIterator(rowColumn, None, valueColumn, None)
      }
    }
  }
  override def keyedRequest(query: KeyedQuery): Future[Iterator[Row]] = {
    futurePool {
      val key = query.keys.find(rowcol => rowcol.column.columnName == rowKeyColumnName).map(_.value)
      key.map { mkey =>
        val value = resolver.getBlob(mkey.asInstanceOf[T])
        new OneHDFSBlobIterator(rowColumn, Some(mkey), valueColumn, value)
      }.getOrElse {
        new OneHDFSBlobIterator(rowColumn, None, valueColumn, None)
      }
    }    
  }
}

object HDFSQueryableSource {
  
  val rowKeyColumnName = "KEY"
  val valueColumnName = "VALUE"
  val allColumnNames = Set(rowKeyColumnName, valueColumnName)
  
  def getRowKeyColumns(ti: TableIdentifier): Set[Column] = Set(Column(rowKeyColumnName, ti))
  def getValueColumn(ti: TableIdentifier): Set[Column] = Set(Column(valueColumnName, ti))
  def getAllColumns(ti: TableIdentifier): Set[Column] = allColumnNames.map(name => Column(name, ti))
  
  
}


  class OneHDFSBlobIterator(keyCol: Column, key: Option[Any], col: Column, value: Option[Array[Byte]]) extends Iterator[Row] with LazyLogging {
    var retrieved = value.isEmpty
    override def hasNext: Boolean = !retrieved
    override def next(): Row = {
      retrieved = true
      value.map { entry =>
        val entries = new ArrayBuffer[RowColumn[_]]()
        key.foreach(k => entries += ColumnFactory.getColumnFromValue(keyCol, k))
        entries += new ArrayByteColumn(col, entry)
 logger.info(s" found and fetched value $value")        

        SimpleRow(entries)
      }.getOrElse(null)
    }
  }
