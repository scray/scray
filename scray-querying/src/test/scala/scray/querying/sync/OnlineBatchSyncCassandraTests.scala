package scray.querying.costs

import scala.annotation.tailrec
import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.SimpleStatement
import scray.querying.description.Row
import scray.querying.sync.types.DbSession
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import com.datastax.driver.core.Cluster
import org.scalatest.BeforeAndAfter
import scray.querying.sync.types.DataTable
import scray.querying.sync.types.Column
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.Statement
import scray.querying.sync.cassandra.OnlineBatchSyncCassandra
import scray.querying.sync.types.ArbitrarylyTypedRows
import scray.querying.sync.types.ColumnWithValue
import scray.querying.sync.cassandra.CassandraImplementation._
import scray.querying.sync.cassandra.OnlineBatchSyncCassandra

@RunWith(classOf[JUnitRunner])
class OnlineBatchSyncTests extends WordSpec with BeforeAndAfter {
  var dbconnection: Option[DbSession[Statement, Insert, ResultSet]] = None
  
  before {
        dbconnection = Option(new DbSession[Statement,Insert, ResultSet]("127.0.0.1") {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE)
        val cassandraSession = Cluster.builder().addContactPoint("127.0.0.1").withPort(EmbeddedCassandraServerHelper.getNativeTransportPort).build().connect()
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()

        override def execute(statement: String): ResultSet = {
          cassandraSession.execute(statement)
        }

        def execute(statement: Statement): ResultSet = {
          cassandraSession.execute(statement)
        }

        def insert(statement: Insert): ResultSet = {
          cassandraSession.execute(statement)
        }
      })
  }

  after {
    // EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
  }
  "OnlineBatchSync " should {
    " init client" in {
      //clean()
      
      class SumColumns() extends ArbitrarylyTypedRows {
        val sum = new Column[Long]("sum")

        override val columns = sum :: Nil
        override val primaryKey = s"(${sum.name})"
        override val indexes: Option[List[String]] = None
      }
      
      val table = new OnlineBatchSyncCassandra("", dbconnection)
      table.initJobClient[SumColumns]("job55", 3, new SumColumns)
    }
    " throw exception if job already exists" in {
      //clean()
      
      class SumColumns() extends ArbitrarylyTypedRows {
        val sum = new Column[Long]("sum")

        override val columns = sum :: Nil
        override val primaryKey = s"(${sum.name})"
        override val indexes: Option[List[String]] = None
      }
      
      val table = new OnlineBatchSyncCassandra("", dbconnection)
      table.initJobClient("job56", 3, new SumColumns())
      
      try {
          table.initJobClient("job56", 3, new SumColumns())
      } catch {
        case _: IllegalStateException => clean
      }
    }
//   "lock table" in {
//     clean()
//      val table = new OnlineBatchSyncCassandra[SumDataColumns]("", dbconnection)
//      table.initJobClient("job55", 3, new SumDataColumns(1456402973L, 1L))
//
//      table.lockOnlineTable("job55", 1)
//      table.selectAll()
//      assert(table.isOnlineTableLocked("job55", 1) === true)
//      assert(table.isOnlineTableLocked("job55", 2) === false)
//    }
//   "insert and read data" in {
//      clean()
//      val table = new OnlineBatchSyncCassandra[SumDataColumns]("", dbconnection)
//      table.initJobClient("job55", 3, new SumDataColumns(1456402973L, 1L))
//      
//      table.unlockOnlineTable("job55", 1)
//      table.insertInOnlineTable("job55", 1, new SumDataColumns(1456402973L, 1L))
//      
//      assert(table.getOnlineJobData("job55", 1).get.time.value === 1456402973L)
//      assert(table.getOnlineJobData("job55", 1).get.sum.value === 1L)
//   }
//    "find latest online batch" in {
//      clean()
//      val table = new OnlineBatchSyncCassandra[SumDataColumns]("", dbconnection)
//      table.initJobClient("job55", 3, new SumDataColumns(1456402973L, 1L))
//      
//       val nr = table.getHeadBatch("job55")
//       assert(table.getOnlineJobData("job55", nr.getOrElse(0)).get.time.value === 1456402973L)
//       assert(table.getOnlineJobData("job55", nr.getOrElse(0)).get.sum.value === 1L)
//    }
  }
  def clean() {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    Thread.sleep(1000)
  }
}