package scray.cassandra.sync

import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec

import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner

import scray.cassandra.sync.CassandraImplementation._
import scray.cassandra.sync.CassandraJobInfo
import scray.cassandra.sync.OnlineBatchSyncCassandra
import scray.cassandra.sync.helpers.TestDbSession
import scray.common.serialization.BatchID
import scray.querying.sync.ArbitrarylyTypedRows
import scray.querying.sync.Column
import scray.querying.sync.MergeMode
import scray.querying.sync.MergeMode
import scray.querying.sync.MergeMode
import scray.querying.sync.RowWithValue
import shapeless.ops.hlist._
import shapeless.syntax.singleton._
import scray.querying.sync.ELEMENT_TIME_BASED
import scray.querying.sync.ColumnWithValue
import scray.querying.sync.ELEMENT_TIME_BASED
import scray.querying.sync.ELEMENT_TIME_BASED
import scala.util.Try
import scray.querying.sync.ELEMENT_TIME_BASED
import scray.querying.sync.State
import scray.querying.sync.START_TIME_BASED

@RunWith(classOf[JUnitRunner])
class StartStopTest extends WordSpec {
  var dbconnection: TestDbSession = new TestDbSession
  var jobNr: AtomicInteger = new AtomicInteger(0)

  def getNextJobName: String = {
    "StartStopTest_Job" + jobNr.getAndIncrement
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

  "StartStopTest " should {
    " init client" in {
      val table = new OnlineBatchSyncCassandra(dbconnection)
      val jobInfo = new CassandraJobInfo(getNextJobName)
      assert(table.initJob[SumTestColumns](jobInfo, new SumTestColumns).isSuccess)
    }
    "get running batch/online version " in {
      val table = new OnlineBatchSyncCassandra(dbconnection)

      val jobInfo = new CassandraJobInfo(getNextJobName)
      val sum = new ColumnWithValue[Long]("sum", 100)
      val columns = sum :: Nil
      val primaryKey = s"(${sum.name})"
      val indexes: Option[List[String]] = None

      table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes))
      assert(table.startNextBatchJob(jobInfo).isSuccess)
      assert(table.completeBatchJob(jobInfo).isSuccess)

      assert(table.startNextOnlineJob(jobInfo).isSuccess)
      assert(table.startNextBatchJob(jobInfo).isSuccess)
      
      assert(table.getRunningBatchJobSlot(jobInfo).get == 2)
      assert(table.getRunningOnlineJobSlot(jobInfo).get == 1)
    }
    "switch to next batch job " in {
      val table = new OnlineBatchSyncCassandra(dbconnection)
      val jobInfo = new CassandraJobInfo(getNextJobName, 4, 4)

      val sum = new ColumnWithValue[Long]("sum", 100)
      val columns = sum :: Nil
      val primaryKey = s"(${sum.name})"
      val indexes: Option[List[String]] = None

      table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes))

      table.startNextBatchJob(jobInfo)
      assert(table.getRunningBatchJobSlot(jobInfo).get === 1)
      table.completeBatchJob(jobInfo)

      table.startNextBatchJob(jobInfo)
      assert(table.getRunningBatchJobSlot(jobInfo).get === 2)
      table.completeBatchJob(jobInfo)

      table.startNextBatchJob(jobInfo)
      assert(table.getRunningBatchJobSlot(jobInfo).get === 3)
      table.completeBatchJob(jobInfo)

      table.startNextBatchJob(jobInfo)
      assert(table.getRunningBatchJobSlot(jobInfo).get === 0)
      table.completeBatchJob(jobInfo)

      table.startNextBatchJob(jobInfo)
      assert(table.getRunningBatchJobSlot(jobInfo).get === 1)
      table.completeBatchJob(jobInfo)

      table.startNextBatchJob(jobInfo)
      assert(table.getRunningBatchJobSlot(jobInfo).get === 2)
      table.completeBatchJob(jobInfo)

      table.startNextBatchJob(jobInfo)
      assert(table.getRunningBatchJobSlot(jobInfo).get === 3)
      table.completeBatchJob(jobInfo)
    }
    "switch to next online job " in {
      val table = new OnlineBatchSyncCassandra(dbconnection)
      val jobInfo = new CassandraJobInfo(getNextJobName, 3, 3)

      val sum = new ColumnWithValue[Long]("sum", 100)
      val columns = sum :: Nil
      val primaryKey = s"(${sum.name})"
      val indexes: Option[List[String]] = None

      table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes))
      
      table.startNextBatchJob(jobInfo)
      table.completeBatchJob(jobInfo)

      table.startNextOnlineJob(jobInfo)
      assert(table.getRunningOnlineJobSlot(jobInfo).get === 1)
      table.completeOnlineJob(jobInfo)

      table.startNextOnlineJob(jobInfo)
      assert(table.getRunningOnlineJobSlot(jobInfo).get === 2)
      table.completeOnlineJob(jobInfo)

      table.startNextOnlineJob(jobInfo)
      assert(table.getRunningOnlineJobSlot(jobInfo).get === 0)
      table.completeOnlineJob(jobInfo)

      table.startNextOnlineJob(jobInfo)
      assert(table.getRunningOnlineJobSlot(jobInfo).get === 1)
      table.completeOnlineJob(jobInfo)
    }
    "start online job and check slot" in {
      val table = new OnlineBatchSyncCassandra(dbconnection)
      val jobInfo = new CassandraJobInfo(getNextJobName)

      val sum = new ColumnWithValue[Long]("sum", 100)
      val columns = sum :: Nil
      val primaryKey = s"(${sum.name})"
      val indexes: Option[List[String]] = None

      table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes))
      table.startNextBatchJob(jobInfo)
      table.completeBatchJob(jobInfo)
      
      table.startNextOnlineJob(jobInfo)

      // Switching is not possible. Because an other job is running.
      assert(table.getRunningBatchJobSlot(jobInfo) == None)
      assert(table.getRunningOnlineJobSlot(jobInfo).get === 1)
    }
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
    " reset batch job " in {
      val table = new OnlineBatchSyncCassandra(dbconnection)
      val job = new CassandraJobInfo(getNextJobName, 3, 3)

      val sum = new ColumnWithValue[Long]("sum", 100)
      val columns = sum :: Nil
      val primaryKey = s"(${sum.name})"
      val indexes: Option[List[String]] = None

      assert(table.initJob(job, new RowWithValue(columns, primaryKey, indexes)).isSuccess)
      assert(table.startNextBatchJob(job).isSuccess)

      
      assert(table.getBatchJobState(job, 1).get.equals(State.RUNNING))
      assert(table.resetBatchJob(job).isSuccess)
      assert(table.getBatchJobState(job, 1).get.equals(State.NEW))
    }
    " reset online job " in {
      val table = new OnlineBatchSyncCassandra(dbconnection)
      val job = new CassandraJobInfo(getNextJobName, 3, 3)

      val sum = new ColumnWithValue[Long]("sum", 100)
      val columns = sum :: Nil
      val primaryKey = s"(${sum.name})"
      val indexes: Option[List[String]] = None

      assert(table.initJob(job, new RowWithValue(columns, primaryKey, indexes)).isSuccess)
      table.startNextBatchJob(job)
      table.completeBatchJob(job)
      assert(table.startNextOnlineJob(job).isSuccess)

      assert(table.getOnlineJobState(job,1 ).get.equals(State.RUNNING))

      assert(table.resetOnlineJob(job).isSuccess)
      assert(table.getOnlineJobState(job, 0).get.equals(State.NEW))
      assert(table.getOnlineJobState(job, 1).get.equals(State.NEW))
      assert(table.getOnlineJobState(job, 2).get.equals(State.NEW))
    }
    " sync stream and batch " in {
      
      val  mergeMode = new ELEMENT_TIME_BASED {
        override  def setFirstElementTime(time: Long): Try[Unit] = {
          Try()
        }
      }
      
      val table = new OnlineBatchSyncCassandra(dbconnection)
      val job = new CassandraJobInfo(getNextJobName, 3, 3)

      val sum = new ColumnWithValue[Long]("sum", 100)
      val columns = sum :: Nil
      val primaryKey = s"(${sum.name})"
      val indexes: Option[List[String]] = None

      assert(table.initJob(job, new RowWithValue(columns, primaryKey, indexes)).isSuccess)
      assert(table.startNextOnlineJob(job).isSuccess) // Start online job and set current start time
//      table.startNextBatchJob(job)

    }
  }
}