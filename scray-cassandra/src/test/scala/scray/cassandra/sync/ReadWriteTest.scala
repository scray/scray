package scray.cassandra.sync

import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.util.Try

import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner

import scray.common.serialization.BatchID
import scray.querying.description.Row
import scray.querying.sync.RowWithValue;
import scray.cassandra.sync.helpers.TestDbSession
import scray.querying.sync.ColumnWithValue
import scray.cassandra.sync.CassandraImplementation._
import shapeless.ops.hlist._
import shapeless.syntax.singleton._

@RunWith(classOf[JUnitRunner])
class ReadWriteTest extends WordSpec {
  var dbconnection: TestDbSession = new TestDbSession
  var jobNr: AtomicInteger = new AtomicInteger(0)

  def getNextJobName: String = {
    "ReadWriteTest_Job" + jobNr.getAndIncrement
  }

  val batchId = new BatchID(1L, 1L)

  "OnlineBatchSync " should {
        "insert and read batch data " in {
          val table = new OnlineBatchSyncCassandra(dbconnection)
          val jobInfo = new CassandraJobInfo(getNextJobName)
    
          val sum = new ColumnWithValue[Long]("sum", 100)
          val columns = sum :: Nil
          val primaryKey = s"(${sum.name})"
          val indexes: Option[List[String]] = None
    
          table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes))
          table.startNextBatchJob(jobInfo)
         
          table.insertInBatchTable(jobInfo, 0, new RowWithValue(columns, primaryKey, indexes)) 
          table.completeBatchJob(jobInfo)
    
          assert(table.getBatchJobData(jobInfo.name, 0, new RowWithValue(columns, primaryKey, indexes)).get.head.columns.head.value === 100L)
        }
        "insert and read online data " in {
          val table = new OnlineBatchSyncCassandra(dbconnection)
          val jobInfo = new CassandraJobInfo(getNextJobName)
    
          val sum = new ColumnWithValue[Long]("sum", 100)
          val columns = sum :: Nil
          val primaryKey = s"(${sum.name})"
          val indexes: Option[List[String]] = None
    
          assert(table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes)).isSuccess)
          table.startNextBatchJob(jobInfo)
          table.completeBatchJob(jobInfo)
          assert(table.startNextOnlineJob(jobInfo).isSuccess)
          assert(table.insertInOnlineTable(jobInfo, 0, new RowWithValue(columns, primaryKey, indexes)).isSuccess) 
          assert(table.completeOnlineJob(jobInfo).isSuccess)
    
          assert(table.getOnlineJobData(jobInfo.name, 0, new RowWithValue(columns, primaryKey, indexes)).get.head.columns.head.value === 100L)
        }
        "red key based " in {
          val table = new OnlineBatchSyncCassandra(dbconnection)
          val jobInfo = new CassandraJobInfo(getNextJobName)
    
          val sum = new ColumnWithValue[Long]("sum", 100)
          val columns = sum :: Nil
          val primaryKey = s"(${sum.name})"
          val indexes: Option[List[String]] = None
    
          assert(table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes)).isSuccess)
          table.startNextBatchJob(jobInfo)
          assert(table.insertInBatchTable(jobInfo, 0, new RowWithValue(columns, primaryKey, indexes)).isSuccess) 
          table.completeBatchJob(jobInfo)
    
          assert(table.getBatchJobData(jobInfo.name, 0, 100, new RowWithValue(columns, primaryKey, indexes)).get.columns.head.value === 100)
          assert(table.getBatchJobData(jobInfo.name, 0, 101, new RowWithValue(columns, primaryKey, indexes)) === None)
        }
        "write and retrieve online data" in {
            val table = new OnlineBatchSyncCassandra(dbconnection)
            val jobInfo = new CassandraJobInfo("job59")
      
      
            val sum = new ColumnWithValue[Long]("sum", 100)
            val columns = sum :: Nil
            val primaryKey = s"(${sum.name})"
            val indexes: Option[List[String]] = None
      
            table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes))
              table.startNextBatchJob(jobInfo)
              table.completeBatchJob(jobInfo)
            table.startNextOnlineJob(jobInfo)
            val oVersion = table.getRunningOnlineJobSlot(jobInfo).get
            table.insertInOnlineTable(jobInfo, oVersion, new RowWithValue(columns, primaryKey, indexes)) 
            table.completeOnlineJob(jobInfo)
            
            val version = table.getNewestOnlineSlot(jobInfo).get
            
            assert(table.getOnlineJobData(jobInfo.name, version, new RowWithValue(columns, primaryKey, indexes)).get.head.columns.head.value === sum.value)
          }
          "write and retrieve batch data" in {
            val table = new OnlineBatchSyncCassandra(dbconnection)
      
            val sum = new ColumnWithValue[Long]("sum", 200)
            val columns = sum :: Nil
            val primaryKey = s"(${sum.name})"
            val indexes: Option[List[String]] = None
            val jobInfo = new CassandraJobInfo(getNextJobName)
      
            assert(table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes)).isSuccess)
            assert(table.startNextBatchJob(jobInfo).isSuccess)
            
            val version = table.getRunningBatchJobSlot(jobInfo).get
            table.insertInBatchTable(jobInfo, version, new RowWithValue(columns, primaryKey, indexes))
            table.completeBatchJob(jobInfo)
            
            assert(table.getBatchJobData(jobInfo.name, version, new RowWithValue(columns, primaryKey, indexes)).get.head.columns.head.value === sum.value)
          }
          "get TableIdentifier of running job" in {
            val table = new OnlineBatchSyncCassandra(dbconnection)
      
            val sum = new ColumnWithValue[Long]("sum", 200)
            val columns = sum :: Nil
            val primaryKey = s"(${sum.name})"
            val indexes: Option[List[String]] = None
            val jobInfo = new CassandraJobInfo(getNextJobName)
      
            assert(table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes)).isSuccess)
            assert(table.startNextBatchJob(jobInfo).isSuccess)         
          }
          "get online start time " in {
            val table = new OnlineBatchSyncCassandra(dbconnection)
            
            val jobInfo = new CassandraJobInfo(getNextJobName)
            val startTime = table.getOnlineStartTime(jobInfo)
            assert(startTime == None)
          }
  }
}