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

class ReadServiceImplSpecs extends WordSpec with BeforeAndAfter with LazyLogging {
  val pathToWinutils = classOf[ReadServiceImplSpecs].getClassLoader.getResource("HADOOP_HOME/bin/winutils.exe");
  val hadoopHome = Paths.get(pathToWinutils.toURI()).toFile().toString().replace("\\bin\\winutils.exe", "")
  System.setProperty("hadoop.home.dir", hadoopHome)

  val exampleFile = s"file:///${System.getProperty("user.dir")}/target/ReadServiceImplSpecs/listFiles/${UUID.randomUUID()}/file1.txt"

  // Write a test file
  before {
    val service = new WriteServiceImpl
    service.writeRawFile(exampleFile, new ByteArrayInputStream(s"ABCDEFG".getBytes))
  }

  "ReadServiceImplSpecs " should {
    " list files in folder " in {
      val reader = new ReadServiceImpl

      val files = reader.getFileList(exampleFile).get()

      Assert.assertTrue(files.size() == 1);
      Assert.assertTrue(files.get(0) == "file1.txt");
    }
    " read file " in {
      val reader = new ReadServiceImpl

      val fileContent = IOUtils.toString(reader.getInputStream(exampleFile).get)
      Assert.assertEquals("ABCDEFG", fileContent);
    }
   " delete file " in {
     // Create example file
     val exampleFile = s"file:///${System.getProperty("user.dir")}/target/ReadServiceImplSpecs/listFiles/${UUID.randomUUID()}/file2.txt"
     val service = new WriteServiceImpl
     service.writeRawFile(exampleFile, new ByteArrayInputStream(s"ABCDEFG".getBytes))
 
     val reader = new ReadServiceImpl
     
     // Check if file exits
     val files = reader.getFileList(exampleFile).get()
     Assert.assertTrue(files.size() == 1);
     
     // Delete file
     reader.deleteFile(exampleFile).get
     
     // Check if file was removed
     Assert.assertTrue(reader.getFileList(exampleFile.replace("file2.txt", "")).get().size() == 0);      
   }
  }
}