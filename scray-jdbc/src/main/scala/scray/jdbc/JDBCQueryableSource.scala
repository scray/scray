package scray.jdbc

import java.sql.ResultSet

import com.twitter.concurrent.Spool
import com.twitter.concurrent.Spool.syntax
import com.twitter.util.Closable
import com.twitter.util.Future

import scray.jdbc.rows.JDBCRow
import scray.querying.description.Row
import scray.querying.description.TableIdentifier
import scray.querying.queries.DomainQuery
import scray.querying.queries.KeyedQuery
import scray.querying.description.ColumnConfiguration
import com.twitter.util.FuturePool
import scray.querying.source.store.QueryableStoreSource
import scray.querying.description.{ Column, ColumnConfiguration, Row, TableIdentifier }
import java.sql.Connection
import scray.jdbc.extractors.DomainToSQLQueryMapping
import scray.querying.description.RowColumn
import scray.querying.description.SimpleRow
import scala.collection.mutable.ArrayBuffer
import scray.jdbc.extractors.ScraySQLDialect

class JDBCQueryableSource[Q <: DomainQuery](
    val ti: TableIdentifier,
    rowKeyColumns: Set[Column],
    clusteringKeyColumns: Set[Column],
    allColumns: Set[Column],
    columnConfigs: Set[ColumnConfiguration],
    val connection: Connection,
    val queryMapper: DomainToSQLQueryMapping[Q, JDBCQueryableSource[Q]],
    futurePool: FuturePool,
    rowMapper: JDBCRow => Row,
    dialect: ScraySQLDialect) extends QueryableStoreSource[Q](ti, rowKeyColumns, clusteringKeyColumns, allColumns, false) {

  val mappingFunction = queryMapper.getQueryMapping(this, Some(ti.tableId), dialect)
  val autoIndexedColumns = columnConfigs.filter(colConf => colConf.index.isDefined &&
    colConf.index.get.isAutoIndexed && colConf.index.get.isSorted).map { colConf =>
    (colConf.column, colConf.index.map(index => index.isAutoIndexed && index.isSorted))
  }

  @inline def requestIterator(query: Q): Future[Iterator[Row]] = {
    import scala.collection.convert.decorateAsScala.asScalaIteratorConverter
    futurePool {
      val queryString = mappingFunction(query)
      val prep = connection.prepareStatement(queryString._1)
      queryMapper.mapWhereClauseValues(prep, query.asInstanceOf[DomainQuery].domains)
      val resultSet = prep.executeQuery(queryString._1)
      val resultMetadata = resultSet.getMetaData 
      (1 to resultMetadata.getColumnCount).map(number => resultMetadata.) 
      getIterator(resultSet)
    }
  }

  override def request(query: Q): Future[Spool[Row]] = {
    requestIterator(query).flatMap(it => JDBCQueryableSource.toRowSpool(it))
  }

  override def keyedRequest(query: KeyedQuery): Future[Iterator[Row]] = {
    requestIterator(query.asInstanceOf[Q])
  }

  override def isOrdered(query: Q): Boolean = query.getOrdering.map { ordering =>
    if (ordering.column.table == ti) {
      // if we want to check whether the data is ordered according to query Q we need to make sure that...
      // 1. the first clustering key with the particular order given in the query is identical to the columns name
      clusteringKeyColumns.head.columnName == ordering.column.columnName ||
        // 2. there isn't any Cassandra-Lucene-Plugin column indexed that can be ordered
        autoIndexedColumns.find { colConf => colConf._1 == ordering.column }.isDefined
    } else {
      false
    }
  }.getOrElse(false)

  protected def getIterator(entities: ResultSet) = new Iterator[Row] {
    var fetchedNextRow = false
    var hasNextRow = false

    def hasNext = {
      if (!fetchedNextRow) {
        hasNextRow = entities.next
        fetchedNextRow = true;
      }
      hasNextRow
    }

    def next = {
      if (!fetchedNextRow) {
        entities.next
      }
      
      SimpleRow(ArrayBuffer.empty[RowColumn[_]])
    }
  }
}

object JDBCQueryableSource {

  //def resultSet2Iterator(rSet: ResultSet, ti: TableIdentifier): Iterator[JDBCRow] = new Iterator[JDBCRow] with Closable 
  def toRowSpool(it: Iterator[Row]): Future[Spool[Row]] = Future.value {
    if (it.hasNext)
      it.next *:: toRowSpool(it)
    else
      Spool.empty
  }
}
