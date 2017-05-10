package scray.cassandra.sync

import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global

import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner

import com.typesafe.scalalogging.slf4j.LazyLogging

import scray.cassandra.sync.CassandraImplementation._
import scray.cassandra.sync.helpers.TestDbSession
import scray.common.serialization.BatchID
import scray.querying.description.Row
import scray.querying.sync.RowWithValue
import shapeless.ops.hlist._
import shapeless.syntax.singleton._
import scala.collection.mutable.HashMap
import scray.querying.sync.Merge
import scray.querying.sync.ColumnWithValue

@RunWith(classOf[JUnitRunner])
class ReadWriteTest extends WordSpec with LazyLogging {
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
    "write and read batch data with simple api" in {
      val table = new OnlineBatchSyncCassandra(dbconnection)

      val sum = new ColumnWithValue[Long]("sum", 200)
      val columns = sum :: Nil
      val primaryKey = s"(${sum.name})"
      val indexes: Option[List[String]] = None
      val jobInfo = new CassandraJobInfo(getNextJobName)

      assert(table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes)).isSuccess)
      assert(table.startNextBatchJob(jobInfo).isSuccess)

      table.insertInBatchTable(jobInfo, new RowWithValue(columns, primaryKey, indexes))
      table.completeBatchJob(jobInfo)

      assert(table.getBatchJobData(jobInfo, new RowWithValue(columns, primaryKey, indexes)).get.head.columns.head.value === sum.value)
    }
    "write multiple batch data and get latest " in {
      val table = new OnlineBatchSyncCassandra(dbconnection)

      val sum = new ColumnWithValue[Long]("sum", 200)
      val columns = sum :: Nil
      val primaryKey = s"(${sum.name})"
      val indexes: Option[List[String]] = None
      val jobInfo = new CassandraJobInfo(getNextJobName)

      assert(table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes)).isSuccess)

      // Write data first time
      assert(table.startNextBatchJob(jobInfo).isSuccess)
      val firstVersion = table.getRunningBatchJobSlot(jobInfo).get
      table.insertInBatchTable(jobInfo, firstVersion, new RowWithValue(columns, primaryKey, indexes))
      table.completeBatchJob(jobInfo)

      // Write next data
      assert(table.startNextBatchJob(jobInfo).isSuccess)
      val secondVersion = table.getRunningBatchJobSlot(jobInfo).get
      val columns1 = new ColumnWithValue[Long]("sum", 300) :: Nil
      table.insertInBatchTable(jobInfo, secondVersion, new RowWithValue(columns1, primaryKey, indexes))
      table.completeBatchJob(jobInfo)

      val latestBatchVersion = table.getLatestBatch(jobInfo).getOrElse(0)

      assert(table.getBatchJobData(jobInfo.name, latestBatchVersion, new RowWithValue(columns, primaryKey, indexes)).get.head.columns.head.value === 300)
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
    "get online start time, when no job is running " in {
      val table = new OnlineBatchSyncCassandra(dbconnection)

      // Define tables and job info
      val jobInfo = new CassandraJobInfo(getNextJobName)
      val sum = new ColumnWithValue[Long]("sum", 100)
      val columns = sum :: Nil
      val primaryKey = s"(${sum.name})"
      val indexes: Option[List[String]] = None

      // Start online job
      table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes))
      assert(table.startNextOnlineJob(jobInfo).isSuccess)

      table.setOnlineStartTime(jobInfo, 100L)
      val startTime = table.getOnlineStartTime(jobInfo).getOrElse(0L)

      assert(startTime > 0)
    }
    "get online start time of running job " in {
      val table = new OnlineBatchSyncCassandra(dbconnection)

      // Define tables and job info
      val jobInfo = new CassandraJobInfo(getNextJobName)
      val sum = new ColumnWithValue[Long]("sum", 100)
      val columns = sum :: Nil
      val primaryKey = s"(${sum.name})"
      val indexes: Option[List[String]] = None

      // Start online job
      table.initJob(jobInfo, new RowWithValue(columns, primaryKey, indexes))
      assert(table.startNextOnlineJob(jobInfo).isSuccess)

      table.setOnlineStartTime(jobInfo, 100L)
      val startTime = table.getOnlineStartTime(jobInfo).getOrElse(0L)

      assert(startTime > 0)
    }
    "find first element if all nodes sended a time " in {

      val jobInfo = new CassandraJobInfo(getNextJobName, numberOfWorkersV = Some(3))
      val startTimeDetector = new StartTimeDetector(jobInfo, dbconnection)
      startTimeDetector.init

      startTimeDetector.publishLocalStartTime(100)
      assert(startTimeDetector.allNodesVoted == None)

      startTimeDetector.publishLocalStartTime(101)
      assert(startTimeDetector.allNodesVoted == None)

      startTimeDetector.publishLocalStartTime(102)
      assert(startTimeDetector.allNodesVoted == Some(100))
    }

    " wait for first element time" in {

      val jobInfo = new CassandraJobInfo(getNextJobName, numberOfWorkersV = Some(3))
      val startTimeDetector = new StartTimeDetector(jobInfo, dbconnection)
      startTimeDetector.init

      startTimeDetector.publishLocalStartTime(100)

      // Wait one second for results
      assert(startTimeDetector.waitForFirstElementTime(1) == None)
      startTimeDetector.publishLocalStartTime(101)
      startTimeDetector.publishLocalStartTime(102)

      // Wait for five seconds and expect a positive result
      assert(startTimeDetector.waitForFirstElementTime(5) == Some(100))

    }

    " run stream and batch content based start sync example" in {
      // Unordered batch data
      val batchData = (1474333200L, 2) :: (1474333205L, 4) :: (1474333202L, 8) :: (1474333203L, 16) :: (1474333300L, 32) :: Nil

      // Streaming data are ordered by date
      val streamData = (1474333300L, 64) :: (1474333301L, 128) :: (1474333302L, 256) :: (1474333303L, 512) :: Nil

      val jobInfo = new CassandraJobInfo(getNextJobName, numberOfWorkersV = Some(1))
      val batchView = new AtomicInteger
      val streamView = new AtomicInteger

      val streamingJob = new Thread(new Runnable {

        val startTimeDetector = new StartTimeDetector(jobInfo, dbconnection)
        startTimeDetector.init

        def run() {

          streamData.map(streamElement => {
            startTimeDetector.publishLocalStartTimeOnlyOnce(streamElement._1)
            streamView.getAndAdd(streamElement._2)
          })

          streamView
        }
      })

      val batchJob = new Thread(new Runnable {

        val startTimeDetector = new StartTimeDetector(jobInfo, dbconnection)
        startTimeDetector.init

        def run() {
          // Filter all elements which are newer than 1474333300
          val batchResult = batchData.filter(startTimeDetector.waitForFirstElementTime(8).get > _._1)
            .foldLeft(0)((acc, element) => acc + element._2)
          batchView.set(batchResult)
        }
      })

      // Batch job waits for a start time
      batchJob.start
      assert(batchView.get == 0)

      // Streaming job triggers the batch job
      streamingJob.start

      Thread.sleep(10000)

      // Check if batch job aggregated four values only
      assert(batchView.get == 30)
      assert(streamView.get == 960)

    }
    " run content based trigger merge example" in {
      // Unordered batch data (Key,Time, Value)
      val batchData = ("K1", 1474333200L, 2) :: ("K1", 1474333205L, 4) :: ("K1", 1474333202L, 8) :: ("K1", 1474333203L, 16) :: ("K1", 1474333300L, 32) :: Nil

      // Streaming data are ordered by date
      val streamData = ("K1", 1474333300L, 64) :: ("K1", 1474333301L, 128) :: ("K1", 1474333302L, 256) :: ("K1", 1474333303L, 512) :: Nil

      val jobInfo = new CassandraJobInfo(getNextJobName, numberOfWorkersV = Some(1))
      val batchView = new HashMap[String, Int]()
      val streamView = new HashMap[String, Int]()

      val mergeOperator = (a: Int, b: Int) => a + b

      val streamingJob = new Thread(new Runnable {

        val startTimeDetector = new StartTimeDetector(jobInfo, dbconnection)
        startTimeDetector.init
        var streamSum = 0 // Sum of processed stream values

        def run() {
          streamData.map(streamElement => {
            startTimeDetector.publishLocalStartTimeOnlyOnce(streamElement._2)
            Thread.sleep(8000) // Sleep to get new batch results
            streamSum += streamElement._3
            streamView.put("K1",
              Merge.merge(
                (streamElement._1, streamSum),
                mergeOperator,
                (key: String) => batchView.get(key).get))
          })
        }
      })

      val batchJob = new Thread(new Runnable {

        val startTimeDetector = new StartTimeDetector(jobInfo, dbconnection)
        startTimeDetector.init

        def run() {
          // Filter all elements which are newer than 1474333300
          val batchResult = batchData.filter(startTimeDetector.waitForFirstElementTime(20).get > _._2)
            .foldLeft(0)((acc, element) => acc + element._3)
          batchView.put("K1", batchResult)
        }
      })

      // Batch job waits for a start time
      batchJob.start
      assert(batchView.get("K1") == None)

      // Streaming job triggers the batch job
      streamingJob.start

      Thread.sleep(40000)

      // Check if batch job aggregated four values only
      assert(batchView.get("K1") == Some(30))
      assert(streamView.get("K1") == Some(990))
    }
  }
}