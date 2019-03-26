// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scray.hdfs.io.osgi

import org.scalatest.WordSpec
import com.typesafe.scalalogging.LazyLogging
import java.io.ByteArrayInputStream
import scray.hdfs.io.index.format.sequence.ValueFileReader
import scray.hdfs.io.index.format.sequence.IdxReader
import java.io.File
import java.util.HashMap
import org.junit.Assert
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputBlob
import com.google.common.util.concurrent.FutureCallback
import com.google.common.util.concurrent.Futures
import java.io.IOException
import java.nio.file.Paths
import scray.hdfs.io.write.WriteResult
import scray.hdfs.io.write.IHdfsWriterConstats.SequenceKeyValueFormat
import scray.hdfs.io.configure.WriteParameter
import java.math.BigInteger
import java.security.PrivilegedActionException
import java.util.UUID
import java.net.URL

class WriteServiceImplSpecs extends WordSpec with LazyLogging {
  val pathToWinutils = classOf[WriteServiceImplSpecs].getClassLoader.getResource("HADOOP_HOME/bin/winutils.exe");
  val hadoopHome = Paths.get(pathToWinutils.toURI()).toFile().toString().replace("\\bin\\winutils.exe", "")
  System.setProperty("hadoop.home.dir", hadoopHome)

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

      getIndexFiles(outPath + "/")
        .map(fileName => {
          if (fileName.startsWith("/")) {
            (new IdxReader("file://" + fileName + ".idx", new OutputBlob),
              new ValueFileReader("file://" + fileName + ".blob", new OutputBlob))
          } else {
            (new IdxReader("file:///" + fileName + ".idx", new OutputBlob),
              new ValueFileReader("file:///" + fileName + ".blob", new OutputBlob))
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
    " handle write url error " in {
      val service = new WriteServiceImpl

      val outPath = "chicken://target/WriteServiceImplSpecs/creatRedrive/" + System.currentTimeMillis() + "/"
      val writtenData = new HashMap[String, Array[Byte]]();

      val writerId = service.createWriter(outPath)

      val writeResult = service.insert(writerId, "42", System.currentTimeMillis(), new ByteArrayInputStream(s"ABCDEFG".getBytes))

      Futures.addCallback(
        writeResult,
        new FutureCallback[WriteResult]() {

          // Should not happen
          override def onSuccess(result: WriteResult) {
            fail
          }

          override def onFailure(t: Throwable) {
            Assert.assertTrue(t.isInstanceOf[PrivilegedActionException])
          }
        });
    }
    "check written bytes value " in {
      val writeService = new WriteServiceImpl();

      val config = new (WriteParameter.Builder)
        .setPath("target/WriteServiceImplSpecs/")
        .setFileFormat(SequenceKeyValueFormat.SEQUENCEFILE_TEXT_TEXT)
        .setMaxNumberOfInserts(3)
        .createConfiguration

      val writerId = writeService.createWriter(config);

      Assert.assertEquals(153, writeService.insert(writerId, "k1", System.currentTimeMillis(), "A".getBytes).get.bytesInserted)
      Assert.assertEquals(185, writeService.insert(writerId, "k1", System.currentTimeMillis(), "A".getBytes).get.bytesInserted)
      Assert.assertEquals(217, writeService.insert(writerId, "k1", System.currentTimeMillis(), "A".getBytes).get.bytesInserted)
            
      Assert.assertEquals(153, writeService.insert(writerId, "k1", System.currentTimeMillis(), new ByteArrayInputStream("A".getBytes), 2048).get.bytesInserted)
      Assert.assertEquals(185, writeService.insert(writerId, "k1", System.currentTimeMillis(), new ByteArrayInputStream("A".getBytes), 2048).get.bytesInserted)
      Assert.assertEquals(217, writeService.insert(writerId, "k1", System.currentTimeMillis(), new ByteArrayInputStream("A".getBytes), 2048).get.bytesInserted)
      
      Assert.assertEquals(153, writeService.insert(writerId, "k1", System.currentTimeMillis(), new ByteArrayInputStream("A".getBytes), new BigInteger("2048"), 2048).get.bytesInserted)
      Assert.assertEquals(185, writeService.insert(writerId, "k1", System.currentTimeMillis(), new ByteArrayInputStream("A".getBytes), new BigInteger("2048"), 2048).get.bytesInserted)
      Assert.assertEquals(217, writeService.insert(writerId, "k1", System.currentTimeMillis(), new ByteArrayInputStream("A".getBytes), new BigInteger("2048"), 2048).get.bytesInserted) 
    }
       " delete file " in {
     if(!System.getProperty("os.name").toUpperCase().contains("WINDOWS")) {
       // Create example file
       val exampleFile =s"${new URL("file:///" + System.getProperty("user.dir"))}" + s"/target/ReadServiceImplSpecs/listFiles/${UUID.randomUUID()}/file2.txt"
       val service = new WriteServiceImpl
       service.writeRawFile(exampleFile, System.getProperty("user.name"), new ByteArrayInputStream(s"ABCDEFG".getBytes))
   
       val reader = new ReadServiceImpl
       
       // Check if file exits
       val files = reader.getFileList(exampleFile).get()
       Assert.assertTrue(files.size() == 1);
       
       // Delete file
       service.deleteFile(exampleFile, System.getProperty("user.name")).get
       
       // Check if file was removed
       Assert.assertEquals(0, reader.getFileList(exampleFile.replace("file2.txt", "")).get().size());    
     } else {
       logger.warn("Delete test was skipped because deleting files on windows is currently not supported")
     }
   }
  }

  private def getIndexFiles(path: String): List[String] = {
    val file = new File(path)
    println(path)
    file.listFiles()
      .map(file => file.getAbsolutePath)
      .filter(filename => filename.endsWith(".idx"))
      .map(idxFile => idxFile.split(".idx")(0))
      .toList
  }
}