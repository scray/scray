package scray.jdbc.automation

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
import scray.jdbc.extractors.DomainToSQLQueryMapping
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

@RunWith(classOf[JUnitRunner])
class DomainToSQLQueryMappingTest extends WordSpec with LazyLogging {

  "DomainToSQLQueryMapping " should {
    " generate SELECT query " in {

      val tableId = new TableIdentifier("oracle", "db1", "table1")
      val column1 = new Column("column1", tableId)

      implicit val ordering = new Ordering[String] { def compare(a: String, b: String) = { 0 } }
      val columnOrdering = new ColumnOrdering(column1, false)
      val domaines = new SingleValueDomain(column1, "abc1", false, false) :: Nil


      val query = new DomainQuery(
        java.util.UUID.randomUUID,
        "queryspace1",
        0,
        Set(column1),
        tableId,
        domaines,
        None,
        Some(columnOrdering),
        Some(QueryRange(None, None, None)))

      val connection = MockitoSugar.mock[Connection]
      val rowMapper = (row: JDBCRow) => SimpleRow(ArrayBuffer.empty[RowColumn[_]])

      val jdbcSource =  new JDBCQueryableSource(
          tableId, 
          Set(column1), 
          Set(), 
          Set(column1), 
          Set.empty[ColumnConfiguration], 
          connection, 
          new DomainToSQLQueryMapping[DomainQuery, JDBCQueryableSource[DomainQuery]], 
          FuturePool(Executors.newCachedThreadPool()), 
          rowMapper)
      
      val mapper2 = new DomainToSQLQueryMapping[DomainQuery, JDBCQueryableSource[DomainQuery]]
      
      println(mapper2.getQueryMapping(jdbcSource, None)(query))
      assert(mapper2.getQueryMapping(jdbcSource, None)(query).toString() === "SELECT * FROM \"db1\".\"table1\" WHERE  \"column1\"=\'abc1\'  ")

    }

  }

}