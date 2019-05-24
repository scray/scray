package scray.hdfs.io.index.format.raw

import org.scalatest.WordSpec
import collection.JavaConverters._

import com.typesafe.scalalogging.LazyLogging
import org.junit.Assert
import java.nio.file.Paths

class RawFileReaderSpecs extends WordSpec with LazyLogging {
  val pathToWinutils = classOf[RawFileWriterSpecs].getClassLoader.getResource("HADOOP_HOME/bin/winutils.exe");
  val hadoopHome = Paths.get(pathToWinutils.toURI()).toFile().toString().replace("\\bin\\winutils.exe", "")
  System.setProperty("hadoop.home.dir", hadoopHome)

  "RawFileReader " should {
    " list files " in {
      val writer = new RawFileWriter("file://ff", "stefan", "".getBytes)
      writer.write("target/rawFileReaderSpecs/file1.raw").close()
      writer.write("target/rawFileReaderSpecs/file2.raw").close()
      writer.write("target/rawFileReaderSpecs/file3.raw").close()

      val reader = new RawFileReader("file://target/rawFileReaderSpecs/", System.getProperty("user.name"))

      val fillist = reader.
        getFileList("target/rawFileReaderSpecs/")
        .get

      fillist.asScala.filter(f => f.getFileName == "file1.raw")

      Assert.assertEquals(1, fillist.asScala.filter(f => f.getFileName == "file1.raw").size)
      Assert.assertEquals(1, fillist.asScala.filter(f => f.getFileName == "file2.raw").size)
      Assert.assertEquals(1, fillist.asScala.filter(f => f.getFileName == "file3.raw").size)
    }
  }
}