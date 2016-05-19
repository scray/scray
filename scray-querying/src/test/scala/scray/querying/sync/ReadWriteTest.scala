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
import scray.querying.sync.helpers.TestDbSession


@RunWith(classOf[JUnitRunner])
class ReadWriteTest extends WordSpec {
  var dbconnection: TestDbSession = new TestDbSession
  var jobNr: AtomicInteger = new AtomicInteger(0)

  def getNextJobName: String = {
    "Job" + jobNr.getAndIncrement
  }

  val batchId = new BatchID(1L, 1L)

  "OnlineBatchSync " should {
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
        " get completed tables " in {
          val table = new OnlineBatchSyncCassandra(dbconnection)
    
          val sum = new ColumnWithValue[Long]("sum", 100)
          val columns = sum :: Nil
          val primaryKey = s"(${sum.name})"
          val indexes: Option[List[String]] = None
    
          table.initJob(new CassandraJobInfo("JOB_100"), new RowWithValue(columns, primaryKey, indexes))
          table.startNextBatchJob(new CassandraJobInfo("JOB_100"))
          table.completeBatchJob(new CassandraJobInfo("JOB_100"))
          
          table.initJob(new CassandraJobInfo("JOB_200"), new RowWithValue(columns, primaryKey, indexes))
          table.startNextBatchJob(new CassandraJobInfo("JOB_200"))
          table.completeBatchJob(new CassandraJobInfo("JOB_200"))
          
          table.initJob(new CassandraJobInfo("JOB_300"), new RowWithValue(columns, primaryKey, indexes))
          table.startNextBatchJob(new CassandraJobInfo("JOB_300"))
          table.completeBatchJob(new CassandraJobInfo("JOB_300"))
          
          table.initJob(new CassandraJobInfo("JOB_400"), new RowWithValue(columns, primaryKey, indexes))
          table.startNextBatchJob(new CassandraJobInfo("JOB_400"))
          table.completeBatchJob(new CassandraJobInfo("JOB_400"))
          
          table.startNextBatchJob(new CassandraJobInfo("JOB_400"))
          table.completeBatchJob(new CassandraJobInfo("JOB_400"))
         
          val expectedResults = List("JOB_100_batch1", "JOB_300_batch1", "JOB_200_batch1", "JOB_400_batch2", "job59_online1", "Job0_online1", "Job1_batch1")
          table.getQueryableTableIdentifiers.map{tableIdentifier => assert(expectedResults.contains(tableIdentifier._2.tableId))}
        }
  }
}