package scray.querying.sync

import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.Insert

import scray.common.serialization.BatchID
import scray.querying.description.Row
import scray.querying.sync._
import scray.querying.sync.cassandra.CassandraImplementation._
import scray.querying.sync.cassandra.CassandraImplementation.RichBoolean
import scray.querying.sync.cassandra.OnlineBatchSyncCassandra
import scray.querying.sync.cassandra.OnlineBatchSyncCassandra
import scray.querying.sync.types.ArbitrarylyTypedRows
import scray.querying.sync.types.Column
import scray.querying.sync.types.DbSession
import scray.querying.sync.types.RowWithValue
import shapeless.ops.hlist._
import shapeless.syntax.singleton._
import scray.querying.sync.cassandra.CassandraJobInfo
import java.util.concurrent.TimeUnit
import scray.querying.sync.types.ColumnWithValue
import scray.querying.sync.types.State


@RunWith(classOf[JUnitRunner])
class OnlineBatchSyncTests extends WordSpec {
  var dbconnection: TestDbSession = new TestDbSession
  var jobNr: AtomicInteger = new AtomicInteger(0)

  def getNextJobName: String = {
    "Job" + jobNr.getAndIncrement
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

  val batchId = new BatchID(1L, 1L)

  class TestDbSession extends DbSession[Statement, Insert, ResultSet]("127.0.0.1") {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE)
    val cassandraSession = Cluster.builder().addContactPoint("127.0.0.1").withPort(EmbeddedCassandraServerHelper.getNativeTransportPort).build().connect()

    override def execute(statement: String): Try[ResultSet] = {
      val result = cassandraSession.execute(statement)
      if (result.wasApplied()) {
        Success(result)
      } else {
        Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}"))
      }
    }

    def execute(statement: Statement): Try[ResultSet] = {
      val result = cassandraSession.execute(statement)
      if (result.wasApplied()) {
        Success(result)
      } else {
        Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}"))
      }
    }

    def insert(statement: Insert): Try[ResultSet] = {
      val result = cassandraSession.execute(statement)
      if (result.wasApplied()) {
        Success(result)
      } else {
        Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Error: ${result.getExecutionInfo}"))
      }
    }
  }

  "OnlineBatchSync " should {
    //          " init client" in {
    //            val table = new OnlineBatchSyncCassandra(dbconnection)
    //            val jobInfo = new CassandraJobInfo(getNextJobName)
    //            assert(table.initJob[SumTestColumns](jobInfo, new SumTestColumns).isSuccess)
    //          }
    //          "lock job" in {
    //            val jobInfo = CassandraJobInfo(getNextJobName)
    //            val table = new OnlineBatchSyncCassandra(dbconnection)
    //            table.initJob(jobInfo, new SumTestColumns())
    //      
    //            jobInfo.getLock(dbconnection).lock
    //            assert(true)
    //          }
    //          "lock and unlock " in {
    //            val job1 = new CassandraJobInfo(getNextJobName)
    //            val table = new OnlineBatchSyncCassandra(dbconnection)
    //            table.initJob(job1, new SumTestColumns())
    //            
    //           
    //            assert(job1.getLock(dbconnection).tryLock(100, TimeUnit.MILLISECONDS))
    //            assert(job1.getLock(dbconnection).tryLock(100, TimeUnit.MILLISECONDS) == false)
    //          }
    //        "transaction method test" in {
    //          val job1 = new CassandraJobInfo(getNextJobName)
    //          val table = new OnlineBatchSyncCassandra(dbconnection)
    //          table.initJob(job1, new SumTestColumns())
    //          
    //          val funcOK = () => {Try()}
    //          val funcFail = () => {Failure(new UnableToLockJobError("..."))}
    //      
    //          assert(job1.getLock(dbconnection).transaction(funcOK).isSuccess)
    //          assert(job1.getLock(dbconnection).transaction(funcFail).isFailure)
    //        }
    //      "multiple transactions test" in {
    //        val job1 = new CassandraJobInfo(getNextJobName)
    //        val table = new OnlineBatchSyncCassandra(dbconnection)
    //        table.initJob(job1, new SumTestColumns())
    //        
    //        val funcOK = () => {Try()}
    //        val funcFail = () => {Failure(new UnableToLockJobError("..."))}
    //    
    //        assert(job1.getLock(dbconnection).transaction(funcOK).isSuccess)
    //        assert(job1.getLock(dbconnection).transaction(funcOK).isSuccess)
    //        assert(job1.getLock(dbconnection).transaction(funcOK).isSuccess)
    //        assert(job1.getLock(dbconnection).transaction(funcOK).isSuccess)
    //        assert(job1.getLock(dbconnection).transaction(funcOK).isSuccess)
    //        assert(job1.getLock(dbconnection).transaction(funcFail).isFailure)
    //      }
    //        "insert and read data" in {
    //          val table = new OnlineBatchSyncCassandra(dbconnection)
    //          val jobInfo = new CassandraJobInfo(getNextJobName)
    //          table.initJob(jobInfo, new SumTestColumns())
    //    
    //          val sum = new ColumnWithValue[Long]("sum", 100)
    //          val columns = sum :: Nil
    //          val primaryKey = s"(${sum.name})"
    //          val indexes: Option[List[String]] = None
    //          
    //          table.insertInOnlineTable(jobInfo, 1, new RowWithValue(columns, primaryKey, indexes))
    //    
    //          val columnsR = new ColumnWithValue[Long]("sum", 200) :: Nil
    //          val columValue = table.getOnlineJobData(jobInfo.name, 1, new RowWithValue(columnsR, primaryKey, indexes)).get.head.columns.head.value.toString()
    //    
    //          assert(columValue === "100")
    //        }
    //        "get running batch/online version " in {
    //          val table = new OnlineBatchSyncCassandra("andreas")
    //          
    //          val jobInfo = new CassandraJobInfo(getNextJobName)
    //          val sum = new ColumnWithValue[Long]("sum", 100)
    //          val columns = sum :: Nil
    //          val primaryKey = s"(${sum.name})"
    //          val indexes: Option[List[String]] = None
    //    
    //          table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes))     
    //          table.startNextBatchJob(jobInfo)
    //          table.startNextOnlineJob(jobInfo)
    //     
    //          assert(table.getRunningBatchJobSlot(jobInfo).get == 1)
    //          assert(table.getRunningOnlineJobSlot(jobInfo).get == 1)
    //        }
//    "switch to next batch job " in {
//      val table = new OnlineBatchSyncCassandra(dbconnection)
//      val jobInfo = new CassandraJobInfo(getNextJobName, 4, 4)
//
//      val sum = new ColumnWithValue[Long]("sum", 100)
//      val columns = sum :: Nil
//      val primaryKey = s"(${sum.name})"
//      val indexes: Option[List[String]] = None
//
//      table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes))
//
//      table.startNextBatchJob(jobInfo)
//      assert(table.getRunningBatchJobSlot(jobInfo).get === 1)
//      table.completeBatchJob(jobInfo)
//
//      table.startNextBatchJob(jobInfo)
//      assert(table.getRunningBatchJobSlot(jobInfo).get === 2)
//      table.completeBatchJob(jobInfo)
//
//      table.startNextBatchJob(jobInfo)
//      assert(table.getRunningBatchJobSlot(jobInfo).get === 3)
//      table.completeBatchJob(jobInfo)
//
//      table.startNextBatchJob(jobInfo)
//      assert(table.getRunningBatchJobSlot(jobInfo).get === 0)
//      table.completeBatchJob(jobInfo)
//
//      table.startNextBatchJob(jobInfo)
//      assert(table.getRunningBatchJobSlot(jobInfo).get === 1)
//      table.completeBatchJob(jobInfo)
//
//      table.startNextBatchJob(jobInfo)
//      assert(table.getRunningBatchJobSlot(jobInfo).get === 2)
//      table.completeBatchJob(jobInfo)
//
//      table.startNextBatchJob(jobInfo)
//      assert(table.getRunningBatchJobSlot(jobInfo).get === 3)
//      table.completeBatchJob(jobInfo)
//    }
//    "switch to next online job " in {
//      val table = new OnlineBatchSyncCassandra(dbconnection)
//      val jobInfo = new CassandraJobInfo(getNextJobName, 3, 3)
//
//      val sum = new ColumnWithValue[Long]("sum", 100)
//      val columns = sum :: Nil
//      val primaryKey = s"(${sum.name})"
//      val indexes: Option[List[String]] = None
//
//      table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes))
//
//      table.startNextOnlineJob(jobInfo)
//      assert(table.getRunningOnlineJobSlot(jobInfo).get === 1)
//      table.completeOnlineJob(jobInfo)
//
//      table.startNextOnlineJob(jobInfo)
//      assert(table.getRunningOnlineJobSlot(jobInfo).get === 2)
//      table.completeOnlineJob(jobInfo)
//
//      table.startNextOnlineJob(jobInfo)
//      assert(table.getRunningOnlineJobSlot(jobInfo).get === 0)
//      table.completeOnlineJob(jobInfo)
//
//      table.startNextOnlineJob(jobInfo)
//      assert(table.getRunningOnlineJobSlot(jobInfo).get === 1)
//      table.completeOnlineJob(jobInfo)
//    }
//        "switch to next job while one job is running" in {
//          val table = new OnlineBatchSyncCassandra(dbconnection)
//          val jobInfo = new CassandraJobInfo(getNextJobName)
//    
//          val sum = new ColumnWithValue[Long]("sum", 100)
//          val columns = sum :: Nil
//          val primaryKey = s"(${sum.name})"
//          val indexes: Option[List[String]] = None
//    
//          table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes))     
//          table.startNextBatchJob(jobInfo)
//          table.startNextOnlineJob(jobInfo)
//          
//          // Switching is not possible. Because an other job is running.
//          assert(table.startNextBatchJob(jobInfo).isSuccess === false)
//     
//          assert(table.getRunningBatchJobSlot(jobInfo).get === 1)
//          assert(table.getRunningOnlineJobSlot(jobInfo).get === 1)
//        }
        "get latest batch after first run" in {
          val table = new OnlineBatchSyncCassandra(dbconnection)
          val jobInfo = new CassandraJobInfo(getNextJobName)
    
          val sum = new ColumnWithValue[Long]("sum", 100)
          val columns = sum :: Nil
          val primaryKey = s"(${sum.name})"
          val indexes: Option[List[String]] = None
    
          table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes))     
          
          table.startNextBatchJob(jobInfo)
          table.completeBatchJob(jobInfo)
          
          assert(table.getLatestBatch(jobInfo).get === 1)
        }
        "get latest batch after second run" in {
          val table = new OnlineBatchSyncCassandra(dbconnection)
          val jobInfo = new CassandraJobInfo(getNextJobName)
    
          val sum = new ColumnWithValue[Long]("sum", 100)
          val columns = sum :: Nil
          val primaryKey = s"(${sum.name})"
          val indexes: Option[List[String]] = None
    
          table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes))     
          
          table.startNextBatchJob(jobInfo)
          table.completeBatchJob(jobInfo)
          
          table.startNextBatchJob(jobInfo)
          table.completeBatchJob(jobInfo)
          
          assert(table.getLatestBatch(jobInfo).get === 2)
        }
//        "insert and read batch data " in {
//          val table = new OnlineBatchSyncCassandra(dbconnection)
//          val jobInfo = new CassandraJobInfo(getNextJobName)
//    
//          val sum = new ColumnWithValue[Long]("sum", 100)
//          val columns = sum :: Nil
//          val primaryKey = s"(${sum.name})"
//          val indexes: Option[List[String]] = None
//    
//          table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes))
//          table.startNextBatchJob(jobInfo)
//         
//          
//          table.insertInBatchTable(jobInfo, 0, new RowWithValue(columns, primaryKey, indexes)) 
//          table.completeBatchJob(jobInfo)
//    
//          assert(table.getBatchJobData(jobInfo.name, 0, new RowWithValue(columns, primaryKey, indexes)).get.head.columns.head.value === 100L)
//        }
//        "insert and read online data " in {
//          val table = new OnlineBatchSyncCassandra(dbconnection)
//          val jobInfo = new CassandraJobInfo(getNextJobName)
//    
//          val sum = new ColumnWithValue[Long]("sum", 100)
//          val columns = sum :: Nil
//          val primaryKey = s"(${sum.name})"
//          val indexes: Option[List[String]] = None
//    
//          assert(table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes)).isSuccess)
//          assert(table.startNextOnlineJob(jobInfo).isSuccess)
//          assert(table.insertInOnlineTable(jobInfo, 0, new RowWithValue(columns, primaryKey, indexes)).isSuccess) 
//          assert(table.completeOnlineJob(jobInfo).isSuccess)
//    
//          assert(table.getOnlineJobData(jobInfo.name, 0, new RowWithValue(columns, primaryKey, indexes)).get.head.columns.head.value === 100L)
//        }
//        "write and retrieve online data" in {
//          val table = new OnlineBatchSyncCassandra(dbconnection)
//          val jobInfo = new CassandraJobInfo("job59")
//    
//    
//          val sum = new ColumnWithValue[Long]("sum", 100)
//          val columns = sum :: Nil
//          val primaryKey = s"(${sum.name})"
//          val indexes: Option[List[String]] = None
//    
//          table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes))
//          table.startNextOnlineJob(jobInfo)
//          val oVersion = table.getRunningOnlineJobSlot(jobInfo).get
//          table.insertInOnlineTable(jobInfo, oVersion, new RowWithValue(columns, primaryKey, indexes)) 
//          table.completeOnlineJob(jobInfo)
//          
//          val version = table.getNewestOnlineSlot(jobInfo).get
//          
//          assert(table.getOnlineJobData(jobInfo.name, version, new RowWithValue(columns, primaryKey, indexes)).get.head.columns.head.value === sum.value)
//        }
//        "write and retrieve batch data" in {
//          val table = new OnlineBatchSyncCassandra(dbconnection)
//    
//          val sum = new ColumnWithValue[Long]("sum", 200)
//          val columns = sum :: Nil
//          val primaryKey = s"(${sum.name})"
//          val indexes: Option[List[String]] = None
//          val jobInfo = new CassandraJobInfo(getNextJobName)
//    
//          assert(table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes)).isSuccess)
//          assert(table.startNextBatchJob(jobInfo).isSuccess)
//          
//          val version = table.getRunningBatchJobSlot(jobInfo).get
//          table.insertInBatchTable(jobInfo, version, new RowWithValue(columns, primaryKey, indexes))
//          table.completeBatchJob(jobInfo)
//          
//          assert(table.getBatchJobData(jobInfo.name, version, new RowWithValue(columns, primaryKey, indexes)).get.head.columns.head.value === sum.value)
//        }
//        " mark new batch job version " in {
//          val table = new OnlineBatchSyncCassandra(dbconnection)
//          val job = new CassandraJobInfo(getNextJobName)
//    
//          val sum = new ColumnWithValue[Long]("sum", 100)
//          val columns = sum :: Nil
//          val primaryKey = s"(${sum.name})"
//          val indexes: Option[List[String]] = None
//    
//          assert(table.initJob(job, new RowWithValue(columns, primaryKey, indexes)).isSuccess)
//          assert(table.startNextBatchJob(job).isSuccess)
//    
//          assert(table.getBatchJobState(job).get.equals(State.NEW))
//          assert(table.getBatchJobState(job).get.equals(State.RUNNING))
//          assert(table.getBatchJobState(job).get.equals(State.NEW))
//        }
//        " mark new online job version " in {
//          val table = new OnlineBatchSyncCassandra(dbconnection)
//          val jobInfo = new CassandraJobInfo(getNextJobName, 3, 3)
//    
//          val sum = new ColumnWithValue[Long]("sum", 100)
//          val columns = sum :: Nil
//          val primaryKey = s"(${sum.name})"
//          val indexes: Option[List[String]] = None
//    
//          table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes))
//          table.startNextOnlineJob(jobInfo)
//          
//          assert(table.getOnlineJobState(jobInfo).get.equals(State.NEW))
//          assert(table.getOnlineJobState(jobInfo).get.equals(State.RUNNING))
//          assert(table.getOnlineJobState(jobInfo).get.equals(State.NEW))
//        }
//        " start and stop jobs " in {
//          val table = new OnlineBatchSyncCassandra(dbconnection)
//          val job = new CassandraJobInfo(getNextJobName)
//    
//          val sum = new ColumnWithValue[Long]("sum", 100)
//          val columns = sum :: Nil
//          val primaryKey = s"(${sum.name})"
//          val indexes: Option[List[String]] = None
//    
//          table.initJob(job, new RowWithValue(columns, primaryKey, indexes))
//          table.startNextBatchJob(job)
//          assert(table.getBatchJobState(job).get.equals(State.RUNNING))
//          table.completeBatchJob(job)
//          assert(table.getBatchJobState(job).get.equals(State.COMPLETED))
//        }
//        " get completed tables " in {
//          val table = new OnlineBatchSyncCassandra(dbconnection)
//    
//          val sum = new ColumnWithValue[Long]("sum", 100)
//          val columns = sum :: Nil
//          val primaryKey = s"(${sum.name})"
//          val indexes: Option[List[String]] = None
//    
//          table.initJob(new CassandraJobInfo("JOB_100"), new RowWithValue(columns, primaryKey, indexes))
//          table.startNextBatchJob(new CassandraJobInfo("JOB_100"))
//          table.completeBatchJob(new CassandraJobInfo("JOB_100"))
//          
//          table.initJob(new CassandraJobInfo("JOB_200"), new RowWithValue(columns, primaryKey, indexes))
//          table.startNextBatchJob(new CassandraJobInfo("JOB_200"))
//          table.completeBatchJob(new CassandraJobInfo("JOB_200"))
//          
//          table.initJob(new CassandraJobInfo("JOB_300"), new RowWithValue(columns, primaryKey, indexes))
//          table.startNextBatchJob(new CassandraJobInfo("JOB_300"))
//          table.completeBatchJob(new CassandraJobInfo("JOB_300"))
//          
//          table.initJob(new CassandraJobInfo("JOB_400"), new RowWithValue(columns, primaryKey, indexes))
//          table.startNextBatchJob(new CassandraJobInfo("JOB_400"))
//          table.completeBatchJob(new CassandraJobInfo("JOB_400"))
//          
//          table.startNextBatchJob(new CassandraJobInfo("JOB_400"))
//          table.completeBatchJob(new CassandraJobInfo("JOB_400"))
//         
//          val expectedResults = List("JOB_100_batch1", "JOB_300_batch1", "JOB_200_batch1", "JOB_400_batch2")
//          table.getQueryableTableIdentifiers.map{tableIdentifier => assert(expectedResults.contains(tableIdentifier._2.tableId))}
//        }
//    " get and set state " in {
//      val table = new OnlineBatchSyncCassandra(dbconnection)
//      val job = new CassandraJobInfo(getNextJobName, 3, 3)
//
//      val sum = new ColumnWithValue[Long]("sum", 100)
//      val columns = sum :: Nil
//      val primaryKey = s"(${sum.name})"
//      val indexes: Option[List[String]] = None
//
//      assert(table.initJob(job, new RowWithValue(columns, primaryKey, indexes)).isSuccess)
//      assert(table.startNextBatchJob(job).isSuccess)
//      println(table.getBatchJobState(job))
//      assert(table.getBatchJobState(job).get.equals(State.RUNNING))
//      assert(table.completeBatchJob(job).isSuccess)
//      assert(table.getBatchJobState(job).get.equals(State.COMPLETED))      
//    }
//    " reset batch job " in {
//      val table = new OnlineBatchSyncCassandra(dbconnection)
//      val job = new CassandraJobInfo(getNextJobName, 3, 3)
//
//      val sum = new ColumnWithValue[Long]("sum", 100)
//      val columns = sum :: Nil
//      val primaryKey = s"(${sum.name})"
//      val indexes: Option[List[String]] = None
//
//      assert(table.initJob(job, new RowWithValue(columns, primaryKey, indexes)).isSuccess)
//      assert(table.startNextBatchJob(job).isSuccess)
//
//      assert(table.getBatchJobState(job).get.equals(State.NEW))
//      assert(table.getBatchJobState(job).get.equals(State.RUNNING))
//      assert(table.getBatchJobState(job).get.equals(State.NEW))
//
//      assert(table.resetBatchJob(job).isSuccess)
//
//      assert(table.getBatchJobState(job).get.equals(State.NEW))
//      assert(table.getBatchJobState(job).get.equals(State.NEW))
//      assert(table.getBatchJobState(job).get.equals(State.NEW))
//    }
//        " reset online job " in {
//          val table = new OnlineBatchSyncCassandra(dbconnection)
//          val job = new CassandraJobInfo(getNextJobName, 3, 3)
//    
//          val sum = new ColumnWithValue[Long]("sum", 100)
//          val columns = sum :: Nil
//          val primaryKey = s"(${sum.name})"
//          val indexes: Option[List[String]] = None
//    
//          assert(table.initJob(job, new RowWithValue(columns, primaryKey, indexes)).isSuccess)
//          assert(table.startNextOnlineJob(job).isSuccess)
//          
//          assert(table.getOnlineJobState(job).get.equals(State.NEW))
//          assert(table.getOnlineJobState(job).get.equals(State.RUNNING))
//          assert(table.getOnlineJobState(job).get.equals(State.NEW))
//          
//          assert(table.resetOnlineJob(job).isSuccess)
//          assert(table.getOnlineJobState(job).get.equals(State.NEW))
//          assert(table.getOnlineJobState(job).get.equals(State.NEW))
//          assert(table.getOnlineJobState(job).get.equals(State.NEW))
//        }
//        " get latest Batch " in {
//          val table = new OnlineBatchSyncCassandra(dbconnection)
//    
//          val jobA = new CassandraJobInfo(getNextJobName, new BatchID(1460465100L, 1460465200L), 3, 3)
//          val jobB = new CassandraJobInfo(getNextJobName, new BatchID(1460465100L, 1460465200L), 3, 3)
//    
//    
//          val sum = new ColumnWithValue[Long]("sum", 100)
//          val columns = sum :: Nil
//          val primaryKey = s"(${sum.name})"
//          val indexes: Option[List[String]] = None
//    
//          assert(table.initJob(jobA, new RowWithValue(columns, primaryKey, indexes)).isSuccess)
//          
//          assert(table.startNextBatchJob(jobA).isSuccess)
//          // assert(table.startNextBatchJob(jobB).isSuccess)
//          
//          assert(table.completeBatchJob(jobA).isSuccess)
//          // println(table.getLatestBatch(jobB))
//        }
  }
}