package scray.sync



import org.scalatest.WordSpec
import scray.sync.api.VersionedData
import scala.util.{Try, Success, Failure}

class VersionedDataWriterSpecs extends WordSpec {
  "VersionedDataWriterSpecs" should {
    "handle io errors " in {
      val writer = new VersionedDataWriter("2a02:8071::1", "testVersioned", "table1", Some("{'class': 'NetworkTopologyStrategy', 'DC2': '2', 'DC3': '0'}"))

      writer.write(new VersionedData("batch", System.currentTimeMillis() + "", 1L, "[\"2016-09-01 12:05:00\",1]")) match {
        case Success(v) => fail();
        case Failure(e) => succeed
      }
    }
  }
  
}