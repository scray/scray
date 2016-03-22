package scray.querying.sync

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
import shapeless._
import syntax.singleton._
import scray.querying.sync.types.ColumnWithValue
import shapeless.ops.hlist._
import scray.querying.sync._
import org.scalatest.BeforeAndAfterAll
import java.util.logging.LogManager
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import org.slf4j.bridge.SLF4JBridgeHandler
import scray.querying.sync.types.SyncTable
import scray.querying.sync.types.State

@RunWith(classOf[JUnitRunner])
class OnlineBatchSyncTests extends WordSpec with BeforeAndAfter with BeforeAndAfterAll {
  var dbconnection: Option[DbSession[Statement, Insert, ResultSet]] = None

  override def beforeAll() = {
    LogManager.getLogManager().reset();
  }
  
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
//    " init client" in {
//
//      val table = new OnlineBatchSyncCassandra("", dbconnection)
//      table.createNewJob[SumTestColumns](JobInfo("job55"), new SumTestColumns)
//    }
//    " throw exception if job already exists" in {
//      val table = new OnlineBatchSyncCassandra("", dbconnection)
//      table.createNewJob(JobInfo("job56"), new SumTestColumns())
//
//      try {
//        table.createNewJob(JobInfo("job56"), new SumTestColumns())
//      } catch {
//        case _: IllegalStateException => true
//      }
//    }
//    "lock table" in {
//      val table = new OnlineBatchSyncCassandra("", dbconnection)
//      table.createNewJob(JobInfo("job57"), new SumTestColumns())
//
//      assert(table.lockOnlineTable(JobInfo("job57")) === true)
//      assert(table.isOnlineTableLocked(JobInfo("job57")) === true)
//    }
//    "lock table only once" in {
//      val table = new OnlineBatchSyncCassandra("", dbconnection)
//      table.createNewJob(JobInfo("job57"), new SumTestColumns())
//
//      assert(table.lockOnlineTable(JobInfo("job57")) === true)
//      assert(table.lockOnlineTable(JobInfo("job57")) === false)
//    }
//    "insert and read data" in {
//      val table = new OnlineBatchSyncCassandra("", dbconnection)
//      table.createNewJob(JobInfo("job58"), new SumTestColumns())
//
//      val sum = new ColumnWithValue[Long]("sum", 100)
//      val columns = sum :: Nil
//      val primaryKey = s"(${sum.name})"
//      val indexes: Option[List[String]] = None
//
//      table.insertInOnlineTable(JobInfo("job58"), 1, new RowWithValue(columns, primaryKey, indexes))
//
//      val columnsR = new ColumnWithValue[Long]("sum", 200) :: Nil
//      val columValue = table.getOnlineJobData("job58", 1, new RowWithValue(columnsR, primaryKey, indexes)).get.head.columns.head.value.toString()
//
//      assert(columValue === "100")
//    }
    "get running batch/online version " in {
      val table = new OnlineBatchSyncCassandra("", dbconnection)
      val jobInfo = JobInfo("job59")

      val sum = new ColumnWithValue[Long]("sum", 100)
      val columns = sum :: Nil
      val primaryKey = s"(${sum.name})"
      val indexes: Option[List[String]] = None

      table.createNewJob(jobInfo, new RowWithValue(columns, primaryKey, indexes))     
      table.startNextBatchJob(jobInfo)
      table.startNextOnlineJob(jobInfo)
 
      assert(table.getRunningBatchJobVersion(jobInfo).get == 0)
      assert(table.getRunningOnlineJobVersion(jobInfo).get == 0)
    }
    "switch to next job " in {
      val table = new OnlineBatchSyncCassandra("", dbconnection)
      val jobInfo = JobInfo("job59")

      val sum = new ColumnWithValue[Long]("sum", 100)
      val columns = sum :: Nil
      val primaryKey = s"(${sum.name})"
      val indexes: Option[List[String]] = None

      table.createNewJob(jobInfo, new RowWithValue(columns, primaryKey, indexes))     
      table.startNextBatchJob(jobInfo)
      table.startNextOnlineJob(jobInfo)
      
      // Switching is not possible. Because an other job is running.
      assert(table.startNextBatchJob(jobInfo) == false)
 
      assert(table.getRunningBatchJobVersion(jobInfo).get == 0)
      assert(table.getRunningOnlineJobVersion(jobInfo).get == 0)
    }
    "mark new version " in {
      val table = new OnlineBatchSyncCassandra("", dbconnection)
      val jobInfo = JobInfo("job59")

      val sum = new ColumnWithValue[Long]("sum", 100)
      val columns = sum :: Nil
      val primaryKey = s"(${sum.name})"
      val indexes: Option[List[String]] = None

      table.createNewJob(jobInfo, new RowWithValue(columns, primaryKey, indexes))
      table.startNextBatchJob(jobInfo)
     
      
      table.insertInBatchTable(jobInfo, 0, new RowWithValue(columns, primaryKey, indexes)) 
      table.completeBatchJob(jobInfo)

      assert(table.getOnlineJobData("job59", version, new RowWithValue(columns, primaryKey, indexes)).get.head.columns.head.value === 100L)
    }
//    "find latest online batch" in {
//      val table = new OnlineBatchSyncCassandra("", dbconnection)
//
//      val sum = new ColumnWithValue[Long]("sum", 100)
//      val columns = sum :: Nil
//      val primaryKey = s"(${sum.name})"
//      val indexes: Option[List[String]] = None
//
//      table.createNewJob(JobInfo("job59"), new RowWithValue(columns, primaryKey, indexes))
//
//      val nr = table.getNewestBatchVersion(JobInfo("job59"))
//      table.insertInOnlineTable(JobInfo("job59"), 3, new RowWithValue(columns, primaryKey, indexes))
//      assert(table.getOnlineJobData("job59", nr.getOrElse(0), new RowWithValue(columns, primaryKey, indexes)).get.head.columns.head.value === 100L)
//    }
//    " mark new batch job version " in {
//      val table = new OnlineBatchSyncCassandra("", dbconnection)
//      val syncTable = SyncTable("SILIDX", "SyncTable")
//      val job = JobInfo("JOB_100")
//
//      val sum = new ColumnWithValue[Long]("sum", 100)
//      val columns = sum :: Nil
//      val primaryKey = s"(${sum.name})"
//      val indexes: Option[List[String]] = None
//
//      table.createNewJob(job, new RowWithValue(columns, primaryKey, indexes))
//      table.startNextBatchJob(job) 
//
//      assert(table.getBatchJobState(job, 0).equals(State.RUNNING))
//      assert(table.getBatchJobState(job, 1).equals(State.NEW))
//      assert(table.getBatchJobState(job, 2).equals(State.NEW))
//    }
//    " mark new online job version " in {
//      val table = new OnlineBatchSyncCassandra("", dbconnection)
//      val syncTable = SyncTable("SILIDX", "SyncTable")
//      val jobInfo = JobInfo("JOB_100")
//
//      val sum = new ColumnWithValue[Long]("sum", 100)
//      val columns = sum :: Nil
//      val primaryKey = s"(${sum.name})"
//      val indexes: Option[List[String]] = None
//
//      table.createNewJob(jobInfo, new RowWithValue(columns, primaryKey, indexes))
//      table.startNextOnlineJob(jobInfo)
//      
//      println(table.getOnlineJobState(jobInfo, 0))
//      println(table.getOnlineJobState(jobInfo, 1))
//      println(table.getOnlineJobState(jobInfo, 2))
//      
//      assert(table.getOnlineJobState(jobInfo, 0).equals(State.RUNNING))
//      assert(table.getOnlineJobState(jobInfo, 1).equals(State.NEW))
//      assert(table.getOnlineJobState(jobInfo, 2).equals(State.NEW))
//    }
//    " start and stop jobs " in {
//      val table = new OnlineBatchSyncCassandra("andreas", None)
//      val syncTable = SyncTable("SILIDX", "SyncTable")
//      val job = JobInfo("JOB_100")
//
//      val sum = new ColumnWithValue[Long]("sum", 100)
//      val columns = sum :: Nil
//      val primaryKey = s"(${sum.name})"
//      val indexes: Option[List[String]] = None
//
//      table.createNewJob(job, new RowWithValue(columns, primaryKey, indexes))
//      table.startNextBatchJob(job)
//      assert(table.getBatchJobState(job, 0).equals(State.RUNNING))
//      table.completeBatchJob(job)
//      assert(table.getBatchJobState(job, 0).equals(State.COMPLETED))
//    }
//
//    
//    "use hlist" in {
//      
//      class A {
//        type TextField <: {
//          val nr : Long
//          val r: Int
//          val chicken: Long
//        }
//      }
//      
//      
//      trait B {
//        object printlnMapper extends Poly1 {
//          implicit def default[T] = at[T](a => a.toString)
//        }
//        type C[R] <: Column[R]
//        type F012 = C[String]:: C[Long] :: C[Int] :: HNil 
//        type F1 = Column[String]:: Column[Long] :: Column[Int] :: HNil 
//        def bla(a: F012){}
//        def addRow(a: F012)(implicit ev0: Mapper[printlnMapper.type, F012])
//      }
//      
//      class R extends B {
//        type C[Int] = Column[Int]
//        
//        override def addRow(a: F012)(implicit ev0: Mapper[printlnMapper.type, F012]) {
//          a.map(printlnMapper)
//        }
//      }
//      
//      val a = new R
//      
//      val f1 = new ColumnWithValue[String]("abc", "abc") :: new ColumnWithValue[Long]("abc", 1L) :: new ColumnWithValue[Int]("abc", 1) :: HNil
//      
//      object printlnMapper extends Poly1 {
//        implicit def default[T] = at[T](a => a.toString)
//      }
//      
//      // a.addRow(f1)(implicit ev0: Mapper[a.printlnMapper.type, a.F012])
//
//      
//     
//      
//      object cyz extends ~>> {
//        def at[Int ∨ String] = 
//      }
//      
//      val b = new B(implicit ev0: Mapper[F2, ColumnWithValue, F1]) {
//        type C[R] = Column[R]
//        val p: F0 = new Column[String]("abc") :: new Column[Long]("ff") :: new Column[Int]("a") :: HNil
//        val s: F1 = new ColumnWithValue[String]("abc", "abc") :: new ColumnWithValue[Long]("abc", 1L) :: new ColumnWithValue[Int]("abc", 1) :: HNil
//        type F2 = String :: Long :: Int :: HNil 
//        def addRow(row: F2)  
//      }
//      p.m
//      
//      b.blub(p)
//      
//    }
  }
}