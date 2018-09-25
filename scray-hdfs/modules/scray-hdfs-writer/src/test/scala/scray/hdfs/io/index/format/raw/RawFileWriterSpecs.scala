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

package scray.hdfs.io.index.format.raw



import org.scalatest.WordSpec

import com.typesafe.scalalogging.LazyLogging
import java.io.ByteArrayInputStream
import org.junit.Assert
import scala.io.Source
import scray.hdfs.io.index.format.raw.RawFileReader;
import scray.hdfs.io.index.format.raw.RawFileWriter;
import java.nio.file.Paths

class RawFileWriterSpecs extends WordSpec with LazyLogging {
  
  val pathToWinutils = classOf[RawFileWriterSpecs].getClassLoader.getResource("HADOOP_HOME/bin/winutils.exe");
  val hadoopHome = Paths.get(pathToWinutils.toURI()).toFile().toString().replace("\\bin\\winutils.exe", "")
  System.setProperty("hadoop.home.dir", hadoopHome)
      
  "RawFileWriter " should {
    " read and write with java.io stream " in {
      
      val dataToWrite = "Chicken42"
      
      val writer = new RawFileWriter("file://ff")
      val outputStream = writer.write("target/rawFileWriterSpecs/useStream.raw")
      outputStream.write(dataToWrite.getBytes)
      outputStream.close();
      
      val reader = new RawFileReader("file://ff")
      val inputStream = reader.read("target/rawFileWriterSpecs/useStream.raw")
      val readDate = Source.fromInputStream(inputStream).mkString
      
      Assert.assertEquals(dataToWrite, readDate)
    }
  }
}