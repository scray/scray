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
import collection.JavaConverters._
import java.util.UUID
import org.scalatest.BeforeAndAfter
import org.apache.commons.io.IOUtils
import java.net.URL
import scray.hdfs.io.write.IHdfsWriterConstats.SequenceKeyValueFormat
import scray.hdfs.io.configure.WriteParameter
import scray.hdfs.io.configure.FixNameCreator

class ReadServiceImplSpecs extends WordSpec with BeforeAndAfter with LazyLogging {
  val pathToWinutils = classOf[ReadServiceImplSpecs].getClassLoader.getResource("HADOOP_HOME/bin/winutils.exe");
  val hadoopHome = Paths.get(pathToWinutils.toURI()).toFile().toString().replace("\\bin\\winutils.exe", "")
  System.setProperty("hadoop.home.dir", hadoopHome)

  val rawExampleFile = s"${new URL("file:///" + System.getProperty("user.dir"))}" + s"/target/ReadServiceImplSpecs/listFiles/${UUID.randomUUID()}/file1.txt"
  val sequenceBytesWritableExampleFile = s"${new URL("file:///" + System.getProperty("user.dir"))}" + s"/target/ReadServiceImplSpecs/readFiles/${UUID.randomUUID()}/"
  val sequenceTextExampleFile = s"${new URL("file:///" + System.getProperty("user.dir"))}" + s"/target/ReadServiceImplSpecs/readFiles/${UUID.randomUUID()}/"



  // Write a test file
  before {
    val service = new WriteServiceImpl
    service.writeRawFile(rawExampleFile, new ByteArrayInputStream(s"ABCDEFG".getBytes), System.getProperty("user.name"), "".getBytes).get()
    service.closeAll
    
    val config = new WriteParameter.Builder()
      .setPath(sequenceBytesWritableExampleFile)
      .setFileFormat(SequenceKeyValueFormat.SEQUENCEFILE_TEXT_BYTESWRITABLE)
      .setMaxNumberOfInserts(12)
      .setFileNameCreator(new FixNameCreator("fileBytesWritable.seq"))
      .createConfiguration
    val writerId1 = service.createWriter(config)
    service.insert(writerId1, "Key42", System.currentTimeMillis(), new ByteArrayInputStream(s"ABCDEFG".getBytes))
    service.close(writerId1)
    
    val config2 = new WriteParameter.Builder()
      .setPath(sequenceBytesWritableExampleFile)
      .setFileFormat(SequenceKeyValueFormat.SEQUENCEFILE_TEXT_TEXT)
      .setMaxNumberOfInserts(12)
      .setFileNameCreator(new FixNameCreator("fileText.seq"))
      .createConfiguration
      
    val writerId2 = service.createWriter(config2)
    service.insert(writerId2, "Key42", System.currentTimeMillis(), "ABCDEFG".getBytes)
    service.close(writerId2)
    
  }

  "ReadServiceImplSpecs " should {
    " list files in folder " in {
      val reader = new ReadServiceImpl

      val files = reader.getFileList(rawExampleFile, System.getProperty("user.name"), "".getBytes).get()

      Assert.assertEquals(1, files.size());
      Assert.assertEquals("file1.txt", files.get(0).getFileName);
    }
    " read file " in {
      val reader = new ReadServiceImpl

      val fileContent = IOUtils.toString(reader.getInputStream(rawExampleFile, System.getProperty("user.name"), "".getBytes).get)
      Assert.assertEquals("ABCDEFG", fileContent);
    }
    " read sequence BytesWritable file" in {
       val reader = new ReadServiceImpl
       
       val id = reader.readFullSequenceFile(sequenceBytesWritableExampleFile + "/fileBytesWritable.seq", SequenceKeyValueFormat.SEQUENCEFILE_TEXT_BYTESWRITABLE, System.getProperty("user.name"), "".getBytes)
      
       Assert.assertTrue(reader.hasNextSequenceFilePair(id).get)   
       val readData = reader.getNextSequenceFilePair(id).get
       Assert.assertEquals("ABCDEFG", new String(readData.getValue))
       Assert.assertTrue(new String(readData.getKey).contains("\"id\": \"Key42\","))
       Assert.assertFalse(reader.hasNextSequenceFilePair(id).get)  
       Assert.assertTrue(null == reader.getNextSequenceFilePair(id))
    }
    " read sequence Text file" in {
       val reader = new ReadServiceImpl
       
       val id = reader.readFullSequenceFile(sequenceBytesWritableExampleFile + "/fileText.seq", SequenceKeyValueFormat.SEQUENCEFILE_TEXT_TEXT, System.getProperty("user.name"), "".getBytes)
      
       Assert.assertTrue(reader.hasNextSequenceFilePair(id).get)   
       val readData = reader.getNextSequenceFilePair(id).get
       Assert.assertEquals("ABCDEFG", new String(readData.getValue))
       Assert.assertTrue(new String(readData.getKey).contains("\"id\": \"Key42\""))
       
       Assert.assertFalse(reader.hasNextSequenceFilePair(id).get)  
       Assert.assertTrue(null == reader.getNextSequenceFilePair(id))
    } 
   " delete file " in {
     if(!System.getProperty("os.name").toUpperCase().contains("WINDOWS")) {
       // Create example file
       val exampleFile =s"${new URL("file:///" + System.getProperty("user.dir"))}" + s"/target/ReadServiceImplSpecs/listFiles/${UUID.randomUUID()}/file2.txt"
       val service = new WriteServiceImpl
       service.writeRawFile(exampleFile, new ByteArrayInputStream(s"ABCDEFG".getBytes), System.getProperty("user.name"), "".getBytes)
   
       val reader = new ReadServiceImpl
       
       // Check if file exits
       val files = reader.getFileList(exampleFile, System.getProperty("user.name"), "".getBytes).get()
       Assert.assertTrue(files.size() == 1);
       
       // Delete file
       reader.deleteFile(exampleFile, "hdfs", "".getBytes).get
       
       // Check if file was removed
       Assert.assertEquals(0, reader.getFileList(exampleFile.replace("file2.txt", ""), System.getProperty("user.name"), "".getBytes).get().size());    
     } else {
       logger.warn("Delete test was skipped because deleting files on windows is currently not supported")
     }
   }
  }
}