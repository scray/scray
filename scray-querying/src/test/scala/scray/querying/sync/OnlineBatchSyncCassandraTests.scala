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
import scray.querying.sync.types.RowWithValue

@RunWith(classOf[JUnitRunner])
class OnlineBatchSyncTests extends WordSpec with BeforeAndAfter {
  var dbconnection: Option[DbSession[Statement, Insert, ResultSet]] = None

  /**
   * Test columns
   */
  class SumTestColumns() extends ArbitrarylyTypedRows {
    val sum = new Column[Long]("sum")

    override val columns = sum :: Nil
    override val primaryKey = s"(${sum.name})"
    override val indexes: Option[List[String]] = None
  }

  before {
    dbconnection = Option(new DbSession[Statement, Insert, ResultSet]("127.0.0.1") {
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
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
  }
  "OnlineBatchSync " should {
    " init client" in {

      val table = new OnlineBatchSyncCassandra("", dbconnection)
      table.initJobClient[SumTestColumns]("job55", 3, new SumTestColumns)
    }
    " throw exception if job already exists" in {
      val table = new OnlineBatchSyncCassandra("", dbconnection)
      table.initJobClient("job56", 3, new SumTestColumns())

      try {
        table.initJobClient("job56", 3, new SumTestColumns())
      } catch {
        case _: IllegalStateException => clean
      }
    }
    "lock table" in {
      val table = new OnlineBatchSyncCassandra("", dbconnection)
      table.initJobClient("job57", 3, new SumTestColumns())

      table.lockOnlineTable("job57", 1)
      assert(table.isOnlineTableLocked("job57", 1) === true)
      assert(table.isOnlineTableLocked("job57", 2) === false)
    }
    "insert and read data" in {
      val table = new OnlineBatchSyncCassandra("", dbconnection)
      table.initJobClient("job58", 3, new SumTestColumns())

      val sum = new ColumnWithValue[Long]("sum", 100)
      val columns = sum :: Nil
      val primaryKey = s"(${sum.name})"
      val indexes: Option[List[String]] = None

      table.insertInOnlineTable("job58", 1, new RowWithValue(columns, primaryKey, indexes))

      val columnsR = new ColumnWithValue[Long]("sum", 200) :: Nil
      val columValue = table.getOnlineJobData("job58", 1, new RowWithValue(columnsR, primaryKey, indexes)).get.head.columns.head.value.toString()

      assert(columValue === "100")
    }
    "find latest online batch" in {
      clean()
      val table = new OnlineBatchSyncCassandra("", dbconnection)

      val sum = new ColumnWithValue[Long]("sum", 100)
      val columns = sum :: Nil
      val primaryKey = s"(${sum.name})"
      val indexes: Option[List[String]] = None

      table.initJobClient("job59", 3, new RowWithValue(columns, primaryKey, indexes))

      val nr = table.getHeadBatch("job59")
      table.insertInOnlineTable("job59", 3, new RowWithValue(columns, primaryKey, indexes))
      assert(table.getOnlineJobData("job59", nr.getOrElse(0), new RowWithValue(columns, primaryKey, indexes)).get.head.columns.head.value === 100L)
    }
  }
  def clean() {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    Thread.sleep(1000)
  }
}