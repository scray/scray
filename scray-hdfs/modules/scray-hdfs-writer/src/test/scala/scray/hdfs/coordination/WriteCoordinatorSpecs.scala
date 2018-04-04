package scray.hdfs.coordination

import org.scalatest.WordSpec

import com.typesafe.scalalogging.LazyLogging
import java.io.ByteArrayInputStream

class WriteCoordinatorSpecs extends WordSpec with LazyLogging {
  "WriteCoordinator " should {
    " wrtite to new file if count limit is reached " in {
      val coordinator = new ReadWriteCoordinatorImpl
          
      val metadata = WriteDestination("000", "target/writeCoordinatorSpecsMaxCount/", IHdfsWriterConstats.FileFormat.SequenceFile, Version(0), 512 * 1024 * 1024L, 5)
      
      val writer = coordinator.getWriter(metadata)
      
      for(i <- 0 to 20) {
        writer.insert("${i}", System.currentTimeMillis(), new ByteArrayInputStream("${i}".getBytes))
      }
      
      
    }
  }
}