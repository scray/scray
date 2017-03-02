package scray.jdbc

import scray.querying.queries.DomainQuery
import scray.querying.source.store.QueryableStoreSource
import scray.querying.description.TableIdentifier
import com.twitter.util.FuturePool
import scray.querying.description.ColumnConfiguration
import scray.querying.description.Column
import scray.querying.description.Row
import scray.querying.queries.KeyedQuery
import com.twitter.concurrent.Spool
import com.twitter.util.Future
import java.sql.ResultSet
import scray.jdbc.rows.JDBCRow
import com.twitter.util.Closable
import scray.jdbc.extractors.DomainToSQLQueryMapping

class JDBCQueryableSource[Q <: DomainQuery] (
      ti: TableIdentifier,
      rowKeyColumns: Set[Column], 
      clusteringKeyColumns: Set[Column],
      allColumns: Set[Column],
      columnConfigs: Set[ColumnConfiguration],
      //????? val session: Session,
      val queryMapper: DomainToSQLQueryMapping[Q, JDBCQueryableSource[Q]],
      futurePool: FuturePool,
      rowMapper: JDBCRow => Row
    ) extends QueryableStoreSource[Q](ti, rowKeyColumns, clusteringKeyColumns, allColumns, false) {
  
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
    requestIterator(query).flatMap(it => JDBCQueryableSource.toRowSpool(it))
  }

  override def keyedRequest(query: KeyedQuery): Future[Iterator[Row]] = {
    requestIterator(query.asInstanceOf[Q])
  }
  
  override def isOrdered(query: Q): Boolean = query.getOrdering.map { ordering =>
    if(ordering.column.table == ti) { 
      // if we want to check whether the data is ordered according to query Q we need to make sure that...
      // 1. the first clustering key with the particular order given in the query is identical to the columns name
      clusteringKeyColumns.head.columnName == ordering.column.columnName ||
      // 2. there isn't any Cassandra-Lucene-Plugin column indexed that can be ordered
        autoIndexedColumns.find { colConf => colConf._1 == ordering.column }.isDefined
    } else {
      false
    }
  }.getOrElse(false)

}

object JDBCQueryableSource {
  
  def resultSet2Iterator(rSet: ResultSet, ti: TableIdentifier): Iterator[JDBCRow] = new Iterator[JDBCRow] with Closable {
  }
  
  def toRowSpool(it: Iterator[Row]): Future[Spool[Row]] = Future.value { 
    if (it.hasNext)
      it.next *:: toRowSpool(it)
    else
      Spool.empty
  }
}
