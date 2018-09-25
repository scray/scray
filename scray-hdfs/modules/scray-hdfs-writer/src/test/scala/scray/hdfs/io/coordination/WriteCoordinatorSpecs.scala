package scray.hdfs.io.coordination

import java.io.File

import org.scalatest.WordSpec

import com.typesafe.scalalogging.LazyLogging
import scray.hdfs.io.index.format.sequence.IdxReader
import scray.hdfs.io.index.format.sequence.BlobFileReader
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputBlob
import java.util.HashMap
import junit.framework.Assert
import java.io.ByteArrayInputStream
import java.nio.file.Paths

class WriteCoordinatorSpecs extends WordSpec with LazyLogging {

  "WriteCoordinator " should {
    
      val pathToWinutils = classOf[WriteCoordinatorSpecs].getClassLoader.getResource("HADOOP_HOME/bin/winutils.exe");
      val hadoopHome = Paths.get(pathToWinutils.toURI()).toFile().toString().replace("\\bin\\winutils.exe", "")
      System.setProperty("hadoop.home.dir", hadoopHome)
      
    " wrtite to new blob file until count limit is reached " in {
      val outPath = "target/WriteCoordinatorSpecs/writeCoordinatorSpecsMaxCount/" + System.currentTimeMillis() + "/"

      val metadata = WriteDestination("000", outPath, IHdfsWriterConstats.FileFormat.SequenceFile, Version(0), 512 * 1024 * 1024L, 5)
      val writer = new CoordinatedWriter(512 * 1024 * 1024L, metadata, new OutputBlob)

      val writtenData = new HashMap[String, Array[Byte]]();

      for (i <- 0 to 20) {
        writer.insert(s"${i}", System.currentTimeMillis(), new ByteArrayInputStream(s"${i}".getBytes))
        writtenData.put(s"${i}", s"${i}".getBytes)
      }

      writer.close;

      val fileName = getIndexFiles(outPath + "/scray-data-000-v0/")
        .map(fileName => {
          
              if(fileName.startsWith("/")) {
                (new IdxReader("file://" + fileName + ".idx", new OutputBlob),
                new BlobFileReader("file://" + fileName + ".blob"))
              } else {
                (new IdxReader("file:///" + fileName + ".idx", new OutputBlob),
                new BlobFileReader("file:///" + fileName + ".blob"))
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

  private def getIndexFiles(path: String): List[String] = {
    val file = new File(path)

    file.listFiles()
      .map(file => file.getAbsolutePath)
      .filter(filename => filename.endsWith(".idx"))
      .map(idxFile => idxFile.split(".idx")(0))
      .toList
  }
}