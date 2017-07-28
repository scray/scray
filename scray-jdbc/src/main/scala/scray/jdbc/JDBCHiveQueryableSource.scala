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
import scray.jdbc.extractors.DomainToHiveSQLQueryMapping
import scray.querying.description.RowColumn
import scray.querying.description.SimpleRow
import scala.collection.mutable.ArrayBuffer
import scray.jdbc.extractors.ScraySQLDialect
import com.zaxxer.hikari.HikariDataSource
import scala.util.Try
import com.typesafe.scalalogging.slf4j.LazyLogging

class JDBCHiveQueryableSource[Q <: DomainQuery](
    val ti: TableIdentifier,
    rowKeyColumns: Set[Column],
    clusteringKeyColumns: Set[Column],
    allColumns: Set[Column],
    columnConfigs: Set[ColumnConfiguration],
    val hikari: HikariDataSource,
    val queryMapper: DomainToHiveSQLQueryMapping[Q, JDBCHiveQueryableSource[Q]],
    futurePool: FuturePool,
    rowMapper: ResultSet => Row,
    dialect: ScraySQLDialect) extends QueryableStoreSource[Q](ti, rowKeyColumns, clusteringKeyColumns, allColumns, false) 
    with LazyLogging {

  val mappingFunction = queryMapper.getQueryMapping(this, Some(ti.tableId), dialect)
  val autoIndexedColumns = columnConfigs.filter(colConf => colConf.index.isDefined &&
    colConf.index.get.isAutoIndexed && colConf.index.get.isSorted).map { colConf =>
    (colConf.column, colConf.index.map(index => index.isAutoIndexed && index.isSorted))
  }

  override def hasSkipAndLimit: Boolean = true
  
  @inline def requestIterator(query: Q): Future[Iterator[Row]] = {
    import scala.collection.convert.decorateAsScala.asScalaIteratorConverter
    futurePool {
      val queryString = mappingFunction(query)
      val connection = hikari.getConnection
      val prep = connection.prepareStatement(queryString._1)
      queryMapper.mapWhereClauseValues(prep, query.asInstanceOf[DomainQuery].domains ++ queryString._3)
      logger.error(s" Executing QUERY NOW=" + queryString._1 + connection.toString())
      val resultSet = prep.executeQuery()
      
      logger.error("result:" + resultSet)
      ///////////////
      def testHiveTable() = { 
    		  logger.error("count=" + resultSet.getMetaData.getColumnCount)
    		  val metadata  = resultSet.getMetaData
    		  val count = metadata.getColumnCount
    		  for( a <- 1 to count){
    			  println( a + ":" + metadata.getColumnLabel(a) + "/" + metadata.getColumnClassName(a) + "/" + 
    					  metadata.getColumnName(a) + "/" + metadata.getColumnType(a) +  "/" + 
    					  metadata.getColumnTypeName(a));
    		  }

    		  logger.error("next:" + resultSet.next() + " | " + resultSet.getRow)

    		  while(resultSet.next()) {
    			  for( a <- 1 to count -1) {
    				  print(resultSet.getString(a) + "|")
    			  }
    			  println(resultSet.getString(count))
    		  }  
      }
      ////////////

logger.error(s" Fetching Iterator NOW")
      getIterator(resultSet, connection)
    }
  }

  override def request(query: Q): Future[Spool[Row]] = {
    requestIterator(query).flatMap(it => JDBCHiveQueryableSource.toRowSpool(it))
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

  protected def getIterator(entities: ResultSet, connection: Connection) = new Iterator[Row] {
    var fetchedNextRow = false
    var hasNextRow = false

    logger.error("getIterator")
   
    
    override def hasNext: Boolean = {
      
      
      
      /* if(entities.isAfterLast()) {
        entities.close()
        connection.close()
      } */
      
      if(entities.isClosed()) {
        hasNextRow = false
      } else {
        if (!fetchedNextRow) {
          
          hasNextRow = entities.next
          fetchedNextRow = true
         
        }
        
      }
      hasNextRow
    }

    override def next: Row = {
      
      
      
      if (!fetchedNextRow) {
        
        hasNextRow = entities.next
        
      }
      fetchedNextRow = false
      rowMapper.apply(entities)
    }
    
    override def finalize(): Unit = {
      
      logger.error("finalize")
      
      Try { if(!entities.isClosed()) {
        entities.close()
      }}
      Try { if(!connection.isClosed()) {
        connection.close()
      }}
    }
  }
}

object JDBCHiveQueryableSource {

  //def resultSet2Iterator(rSet: ResultSet, ti: TableIdentifier): Iterator[JDBCRow] = new Iterator[JDBCRow] with Closable 
  def toRowSpool(it: Iterator[Row]): Future[Spool[Row]] = Future.value {
    if (it.hasNext)
      it.next *:: toRowSpool(it)
    else
      Spool.empty
  }
}
