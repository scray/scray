package scray.hdfs.io.coordination

import java.io.File
import org.scalatest.WordSpec
import com.typesafe.scalalogging.LazyLogging
import scray.hdfs.io.index.format.sequence.IdxReader
import scray.hdfs.io.index.format.sequence.ValueFileReader
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputBlob
import scray.hdfs.io.write.IHdfsWriterConstats

import java.util.HashMap
import junit.framework.Assert
import org.apache.hadoop.fs.InvalidPathException

import java.io.ByteArrayInputStream
import java.nio.file.Paths
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputTextText

import java.util.Optional
import scray.hdfs.io.configure.WriteParameter
import scray.hdfs.io.configure.Version

class WriteCoordinatorSpecs extends WordSpec with LazyLogging {

  "WriteCoordinator " should {

    val pathToWinutils = classOf[WriteCoordinatorSpecs].getClassLoader.getResource("HADOOP_HOME/bin/winutils.exe");
    val hadoopHome = Paths.get(pathToWinutils.toURI()).toFile().toString().replace("\\bin\\winutils.exe", "")
    System.setProperty("hadoop.home.dir", hadoopHome)

    " wrtite to new blob file until count limit is reached " in {
      val outPath = "target/WriteCoordinatorSpecs/writeCoordinatorSpecsMaxCount/" + System.currentTimeMillis() + "/"

      val metadata = new (WriteParameter.Builder)
        .setPath(outPath)
        .setQueryspace("000")
        .setFileFormat(IHdfsWriterConstats.SequenceKeyValueFormat.SEQUENCEFILE_INDEXVALUE_BLOB)
        .setMaxNumberOfInserts(20)
        .setCreateScrayIndexFile(true)
        .setWriteVersioned(true)
        .createConfiguration

      val writer = new CoordinatedWriter(512 * 1024 * 1024L, metadata, new OutputBlob)

      val writtenData = new HashMap[String, Array[Byte]]();

      for (i <- 0 to 20) {
        writer.insert(s"${i}", System.currentTimeMillis(), new ByteArrayInputStream(s"${i}".getBytes))
        writtenData.put(s"${i}", s"${i}".getBytes)
      }

      writer.close;

      val fileName = getIndexFiles(outPath + "/scray-data-000-v0/")
        .map(fileName => {

          if (fileName.startsWith("/")) {
            (new IdxReader("file://" + fileName + ".idx.seq", new OutputBlob),
              new ValueFileReader("file://" + fileName + ".data.seq", new OutputBlob))
          } else {
            (new IdxReader("file:///" + fileName + ".idx.seq", new OutputBlob),
              new ValueFileReader("file:///" + fileName + ".data.seq", new OutputBlob))
          }
        })
        .map {
          case (idxReader, blobReader) => {
            val idx = idxReader.next().get
            val data = blobReader.get(idx.getKey.toString(), idx.getPosition)

            val value = writtenData.get(idx.getKey.toString())
            Assert.assertTrue((new String(data.get)).equals(new String(value)))
          }
        }
    }
  }
  "throw exception if path contains a space " in {
    try {
      new (WriteParameter.Builder)
        .setPath("/tmp/test path/file1.txt")
    } catch {
      case e: InvalidPathException => Assert.assertTrue(true)
      case e: Throwable => Assert.fail("Unexpected exception: " + e.getMessage)
    }
  }
  private def getIndexFiles(path: String): List[String] = {
    println(path)
    val file = new File(path)

    file.listFiles()
      .map(file => file.getAbsolutePath)
      .filter(filename => filename.endsWith(".idx.seq"))
      .map(idxFile => idxFile.split(".idx.seq")(0))
      .toList
  }
}