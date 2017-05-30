package scray.cassandra.sync;

import java.util.logging.LogManager

import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner

import scray.cassandra.sync.CassandraImplementation._
import scray.common.serialization.BatchID
import shapeless.ops.hlist._
import shapeless.syntax.singleton._
import scray.querying.sync.ColumnWithValue
import scray.querying.sync.ArbitrarylyTypedRows
import scray.querying.sync.RowWithValue
import scray.querying.sync.JobInfo
import scray.querying.sync.Column
import scray.querying.sync.JobLockTable
import scray.querying.sync.SyncTableBasicClasses.JobLockTable
import org.apache.commons.lang3.SerializationUtils


@RunWith(classOf[JUnitRunner])
class SerializationSpecs extends WordSpec with BeforeAndAfter with BeforeAndAfterAll {

  override def beforeAll() = {
    LogManager.getLogManager().reset();
  }
  
  "SerializationSpecs " should {
    " serialize CassandraSyncTableLock" in {

      val synctableLockObject = new CassandraSyncTableLock(
          new CassandraJobInfo("job1"),
          JobLockTable("cs1", "JobLockTable"),
          new CassandraDbSession(""),
          100)
      try {
        val serializedObject = SerializationUtils.serialize(synctableLockObject)
      } catch {
        case e: Exception => fail
      }
    }
    " serialize CassandraJobInfo" in {

     val jobInfo = new CassandraJobInfo("job1")

      try {
        val serializedObject = SerializationUtils.serialize(jobInfo)
      } catch {
        case e: Exception => fail
      }
    }
  }
}