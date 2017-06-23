package scray.cassandra.sync;

import java.util.logging.LogManager

import org.apache.commons.lang3.SerializationUtils
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, WordSpec}
import org.scalatest.junit.JUnitRunner
import scray.cassandra.sync.CassandraImplementation._
import scray.querying.sync.JobLockTable


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