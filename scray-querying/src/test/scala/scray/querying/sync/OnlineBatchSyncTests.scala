package scray.querying.costs

import scala.annotation.tailrec
import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.SimpleStatement
import scray.querying.description.Row
import scray.querying.sync.OnlineBatchSyncCassandra
import scray.querying.sync.types.DataColumns
import scray.querying.sync.types.DbSession
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import com.datastax.driver.core.Cluster
import org.scalatest.BeforeAndAfter
import scray.querying.sync.types.DataTable
import scray.querying.sync.types.Column
import scray.querying.sync.types.ColumnV
import scray.querying.sync.types.CassandraTypeName
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.Statement
import scray.querying.sync.types.SumDataColumns

@RunWith(classOf[JUnitRunner])
class OnlineBatchSyncTests extends WordSpec with BeforeAndAfter {
  var dbconnection: Option[DbSession[Statement, Insert, ResultSet]] = None
  
  before {
        dbconnection = Option(new DbSession[Statement,Insert, ResultSet]("127.0.0.1") {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE)
        val cassandraSession = Cluster.builder().addContactPoint("127.0.0.1").withPort(EmbeddedCassandraServerHelper.getNativeTransportPort).build().connect()

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

  after {}
  "OnlineBatchSync " should {
    " " in {
      val table = new OnlineBatchSyncCassandra[SumDataColumns]("", dbconnection)
      table.initJob("job55", 3, new SumDataColumns(1456402973L, 1L))
    }
   "lock table" in {
      val table = new OnlineBatchSyncCassandra("", dbconnection)
      table.selectAll()
      table.lockOnlineTable("job55", 1)
      table.selectAll()
      assert(table.isOnlineTableLocked("job55", 1) === true)
      assert(table.isOnlineTableLocked("job55", 2) === false)
    }
   "insert Data" in {
      val table = new OnlineBatchSyncCassandra("", dbconnection)
      table.selectAll()
      table.unlockOnlineTable("job55", 1)
      table.selectAll()
      table.insertInOnlineTable("job55", 1, new SumDataColumns(1456402973L, 1L))
      
      assert(table.getOnlineJobData("job55", 1).time.value === 1456402973L)
      assert(table.getOnlineJobData("job55", 1).sum.value === 1L)
   }
//    "find latest online batch" in {
//      val table = new OnlineBatchSyncCassandra("", dbconnection)
//      
//      class TestDataColumns(timeV: Long, sumV: Long) extends DataColumns[Insert](timeV) {
//        val sum = new ColumnV[Long]("sum", CassandraTypeName.getCassandraTypeName, sumV)
//        override def getInsertStatement() = {
//          val a = QueryBuilder.insertInto(table)
//        }
//        override val allVals: List[Column[_]] = time :: sum :: Nil
//      }
//    
//            
//      val dataTable = new TestDataTable()
//      dbconnection.get.execute(statement)
//    }

  }

}