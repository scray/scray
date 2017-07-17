package scray.jdbc.extractors

import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.querying.sync.SyncTableBasicClasses.SyncTableRowEmpty
import scray.querying.description.SimpleRow
import scala.collection.mutable.ArrayBuffer
import scray.querying.description.RowColumn
import scray.jdbc.JDBCQueryableSource
import com.twitter.util.Await
import java.sql.Connection
import org.mockito.Mock
import org.mockito.runners.MockitoJUnitRunner
import scray.querying.description.TableIdentifier
import com.twitter.util.FuturePool
import scray.jdbc.rows.JDBCRow
import scray.querying.description.ColumnConfiguration
import scray.querying.description.Column
import scray.querying.queries.DomainQuery
import java.util.concurrent.Executors
import org.scalatest.mock.MockitoSugar
import com.twitter.util.Future
import com.twitter.concurrent.Spool
import scray.querying.description.Row
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.querying.description.QueryRange
import scray.querying.description.ColumnOrdering
import scray.querying.description.internal.SingleValueDomain
import com.zaxxer.hikari.HikariDataSource
import java.sql.ResultSet

@RunWith(classOf[JUnitRunner])
class DomainToSQLQueryMappingSpecs extends WordSpec with LazyLogging {

  "DomainToSQLQueryMapping " should {
    " generate SELECT query " in {

      val tableId = new TableIdentifier("oracle", "DB1", "TABLE1")
      val column1 = new Column("key", tableId)
      val domaines = new SingleValueDomain(column1, "abc1", false, false) :: Nil


      val query = new DomainQuery(
        java.util.UUID.randomUUID,
        "queryspace1",
        0,
        Set(column1),
        tableId,
        domaines,
        None,
        None,
        Some(QueryRange(None, None, None)))

      val connection = MockitoSugar.mock[HikariDataSource]
      val rowMapper = (row: ResultSet) => SimpleRow(ArrayBuffer.empty[RowColumn[_]])

      val jdbcSource =  new JDBCQueryableSource(
          tableId, 
          Set(column1), 
          Set(), 
          Set(column1), 
          Set.empty[ColumnConfiguration], 
          connection, 
          new DomainToSQLQueryMapping[DomainQuery, JDBCQueryableSource[DomainQuery]], 
          FuturePool(Executors.newCachedThreadPool()), 
          rowMapper,
          ScraySQLDialectFactory.getDialect("ORACLE"))
      
      val mapper2 = new DomainToSQLQueryMapping[DomainQuery, JDBCQueryableSource[DomainQuery]]
      
      println(mapper2.getQueryMapping(jdbcSource, None, ScraySQLDialectFactory.getDialect("ORACLE"))(query))
      assert(mapper2.getQueryMapping(jdbcSource, None, ScraySQLDialectFactory.getDialect("ORACLE"))(query).toString() === "(SELECT * FROM \"DB1\".\"TABLE1\" WHERE  key = ?    ,1,List())")

    }

  }

}