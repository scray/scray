package scray.cassandra

import com.datastax.driver.core.{ Row => CassRow, Session }
import com.twitter.concurrent.Spool
import com.twitter.concurrent.Spool.syntax
import com.twitter.util.{ Future, FuturePool }

import scray.cassandra.extractors.DomainToCQLQueryMapping
import scray.querying.description.{ Column, ColumnConfiguration, Row, TableIdentifier }
import scray.querying.queries.DomainQuery
import scray.querying.source.store.QueryableStoreSource
import scray.querying.queries.KeyedQuery

class CassandraQueryableSource[Q <: DomainQuery](
    val ti: TableIdentifier,
    rowKeyColumns: Set[Column], 
    clusteringKeyColumns: Set[Column],
    allColumns: Set[Column],
    columnConfigs: Set[ColumnConfiguration],
    val session: Session,
    val queryMapper: DomainToCQLQueryMapping[Q, CassandraQueryableSource[Q]],
    futurePool: FuturePool,
    rowMapper: CassRow => Row)
      extends QueryableStoreSource[Q](ti, rowKeyColumns, clusteringKeyColumns, allColumns, false) {

  val mappingFunction = queryMapper.getQueryMapping(this, Some(ti.tableId))
  val autoIndexedColumns = columnConfigs.filter(colConf => colConf.index.isDefined && 
      colConf.index.get.isAutoIndexed && colConf.index.get.isSorted).map { colConf =>
    (colConf.column, colConf.index.map(index => index.isAutoIndexed && index.isSorted))
  }
  
  @inline def requestIterator(query: Q): Future[Iterator[Row]] = {
    import scala.collection.convert.decorateAsScala.asScalaIteratorConverter
    futurePool {
      val queryString = mappingFunction(query)
      val resultSet = session.execute(queryString).iterator().asScala
      resultSet.map { cqlRow => 
        rowMapper(cqlRow)
      }
    }
  }
  
  override def request(query: Q): Future[Spool[Row]] = {
    requestIterator(query).flatMap(it => CassandraQueryableSource.toRowSpool(it))
  }

  override def keyedRequest(query: KeyedQuery): Future[Iterator[Row]] = {
    requestIterator(query.asInstanceOf[Q])
  }
  
  override def isOrdered(query: Q): Boolean = query.getOrdering.map { ordering =>
    if(ordering.column.table == ti) { 
      // if we want to check whether the data is ordered according to query Q we need to make sure that...
      // 1. the first clustering key with the particular order given in the query is identical to the columns name
      val clusteringKeyOrder = if(!clusteringKeyColumns.isEmpty) {
        clusteringKeyColumns.head.columnName == ordering.column.columnName
      } else {
        false
      }
       clusteringKeyOrder ||
      // 2. there isn't any Cassandra-Lucene-Plugin column indexed that can be ordered
        autoIndexedColumns.find { colConf => colConf._1 == ordering.column }.isDefined
    } else {
      false
    }
  }.getOrElse(false)

}

object CassandraQueryableSource {
  def toRowSpool(it: Iterator[Row]): Future[Spool[Row]] = Future.value { 
    if (it.hasNext)
      it.next *:: toRowSpool(it)
    else
      Spool.empty
  }
}
