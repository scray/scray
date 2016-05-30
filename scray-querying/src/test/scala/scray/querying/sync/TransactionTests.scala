package scray.querying.sync

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.Insert

import scray.common.serialization.BatchID
import scray.querying.sync.cassandra.CassandraImplementation.genericCassandraColumnImplicit
import scray.querying.sync.cassandra.CassandraJobInfo
import scray.querying.sync.cassandra.OnlineBatchSyncCassandra
import scray.querying.sync.types.ArbitrarylyTypedRows
import scray.querying.sync.types.Column
import scray.querying.sync.types.DbSession
import scray.querying.sync.helpers.TestDbSession
import scray.querying.sync.helpers.SumTestColumns

@RunWith(classOf[JUnitRunner])
class TransactionTests extends WordSpec {
  var dbconnection: TestDbSession = new TestDbSession
  var jobNr: AtomicInteger = new AtomicInteger(0)

  def getNextJobName: String = {
    "Job" + jobNr.getAndIncrement
  }

  "TransactionAPI " should {
    "lock job" in {
      val jobInfo = CassandraJobInfo(getNextJobName)
      val table = new OnlineBatchSyncCassandra(dbconnection)
      table.initJob(jobInfo, new SumTestColumns())

      jobInfo.getLock(dbconnection).lock
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