package scray.hdfs.io.index.format.raw

import org.scalatest.WordSpec
import collection.JavaConverters._

import com.typesafe.scalalogging.LazyLogging
import org.junit.Assert

class RawFileReaderSpecs extends WordSpec with LazyLogging {
    "RawFileReader " should {
      " list files " in {
        val writer = new RawFileWriter("file://ff")
        writer.write("target/rawFileReaderSpecs/file1.raw").close()
        writer.write("target/rawFileReaderSpecs/file2.raw").close()
        writer.write("target/rawFileReaderSpecs/file3.raw").close()

        val reader = new RawFileReader("file://target/rawFileReaderSpecs/")
        
        val fillist = reader.
        getFileList("target/rawFileReaderSpecs/")
        .get
      
        
        Assert.assertTrue(fillist.contains("file1.raw"))
        Assert.assertTrue(fillist.contains("file2.raw"))
        Assert.assertTrue(fillist.contains("file3.raw"))

      }
    }
}