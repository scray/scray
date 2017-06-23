package scray.cassandra.sync

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.cassandra.sync.helpers.{SumTestColumns, TestDbSession}
import scray.querying.sync.UnableToLockJobError
import scray.cassandra.sync.CassandraImplementation._


import scala.util.{Failure, Try}

@RunWith(classOf[JUnitRunner])
class TransactionTests extends WordSpec {
  var dbconnection: TestDbSession = new TestDbSession
  var jobNr: AtomicInteger = new AtomicInteger(0)

  def getNextJobName: String = {
    "TransactionTest_Job" + jobNr.getAndIncrement
  }

  "TransactionAPI " should {
    "lock job" in {
      val jobInfo = CassandraJobInfo(getNextJobName)
      val table = new OnlineBatchSyncCassandra(dbconnection)
      table.initJob(jobInfo, new SumTestColumns())

      jobInfo.getLock(dbconnection)
      assert(true)
    }
    "lock and unlock " in {
      val job1 = new CassandraJobInfo(getNextJobName)
      val table = new OnlineBatchSyncCassandra(dbconnection)
      table.initJob(job1, new SumTestColumns())

      assert(job1.getLock(dbconnection).tryLock(100, TimeUnit.MILLISECONDS))
      assert(job1.getLock(dbconnection).tryLock(100, TimeUnit.MILLISECONDS) == false)
    }
    "use transaction method" in {
      val job1 = new CassandraJobInfo(getNextJobName)
      val table = new OnlineBatchSyncCassandra(dbconnection)
      table.initJob(job1, new SumTestColumns())

      val funcOK = () => { Try() }
      val funcFail = () => { Failure(new UnableToLockJobError("...")) }

      assert(job1.getLock(dbconnection).transaction(funcOK).isSuccess)
      assert(job1.getLock(dbconnection).transaction(funcFail).isFailure)
    }
    "test multiple transactions" in {
      val job1 = new CassandraJobInfo(getNextJobName)
      val table = new OnlineBatchSyncCassandra(dbconnection)
      table.initJob(job1, new SumTestColumns())

      val funcOK = () => { Try() }
      val funcFail = () => { Failure(new UnableToLockJobError("...")) }

      assert(job1.getLock(dbconnection).transaction(funcOK).isSuccess)
      assert(job1.getLock(dbconnection).transaction(funcOK).isSuccess)
      assert(job1.getLock(dbconnection).transaction(funcOK).isSuccess)
      assert(job1.getLock(dbconnection).transaction(funcOK).isSuccess)
      assert(job1.getLock(dbconnection).transaction(funcOK).isSuccess)
      assert(job1.getLock(dbconnection).transaction(funcFail).isFailure)
    }
  }
}