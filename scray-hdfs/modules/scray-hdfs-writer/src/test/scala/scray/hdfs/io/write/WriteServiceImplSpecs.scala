package scray.hdfs.io.write

import org.scalatest.WordSpec

import com.typesafe.scalalogging.LazyLogging

import java.io.ByteArrayInputStream
import scray.hdfs.io.index.format.sequence.BlobFileReader
import scray.hdfs.io.index.format.sequence.IdxReader
import java.io.File
import java.util.HashMap
import org.junit.Assert
import scray.hdfs.io.osgi.WriteServiceImpl
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputBlob
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.FutureCallback
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.AbstractFuture
import com.google.common.util.concurrent.SettableFuture
import java.util.concurrent.Executors
import java.io.IOException

class WriteServiceImplSpecs extends WordSpec with LazyLogging {
  "WriteServiceImplSpecs " should {
    " create and redrive writer " in {
      val service = new WriteServiceImpl

      val outPath = "target/WriteServiceImplSpecs/creatRedrive/" + System.currentTimeMillis() + "/"
      val writtenData = new HashMap[String, Array[Byte]]();

      val writerId = service.createWriter(outPath)

      service.insert(writerId, "42", System.currentTimeMillis(), new ByteArrayInputStream(s"ABCDEFG".getBytes))
      writtenData.put(s"42", s"ABCDEFG".getBytes)

      service.insert(writerId, "43", System.currentTimeMillis(), new ByteArrayInputStream(s"ABCDEFG".getBytes))
      writtenData.put(s"43", s"ABCDEFG".getBytes)

      service.insert(writerId, "44", System.currentTimeMillis(), new ByteArrayInputStream(s"ABCDEFG".getBytes))
      writtenData.put(s"44", s"ABCDEFG".getBytes)

      service.insert(writerId, "45", System.currentTimeMillis(), new ByteArrayInputStream(s"ABCDEFG".getBytes))
      writtenData.put(s"45", s"ABCDEFG".getBytes)

      service.close(writerId)

      getIndexFiles(outPath + "/scray-data-000-v0/")
        .map(fileName => {
          (
            new IdxReader("file://" + fileName + ".idx", new OutputBlob),
            new BlobFileReader("file://" + fileName + ".blob"))
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
    " handle wrote url error " in {
      val service = new WriteServiceImpl

      val outPath = "chicken://target/WriteServiceImplSpecs/creatRedrive/" + System.currentTimeMillis() + "/"
      val writtenData = new HashMap[String, Array[Byte]]();

      val writerId = service.createWriter(outPath)

      val writeResult = service.insert(writerId, "42", System.currentTimeMillis(), new ByteArrayInputStream(s"ABCDEFG".getBytes))
      
      Futures.addCallback(writeResult,
        new FutureCallback[WriteResult]() {
        
        // Should not happen
        override def onSuccess(result: WriteResult) {
          fail   
        }
 
        override def onFailure(t: Throwable) {
           Assert.assertTrue(t.isInstanceOf[IOException])
        }
      });
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