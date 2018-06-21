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

package scray.hdfs.index.format.raw



import org.scalatest.WordSpec

import com.typesafe.scalalogging.LazyLogging
import java.io.ByteArrayInputStream
import org.junit.Assert
import scala.io.Source

class RawFileWriterSpecs extends WordSpec with LazyLogging {

  "RawFileWriter " should {
    " write and read data to file " in {
      val writer = new RawFileWriter("file://ff")

      for (i <- 0 to 10) {
        writer.write(s"/tmp/chickenR_${i}", new ByteArrayInputStream("ABC".getBytes));
      }

    }
    " write to output stream " in {
      
      val dataToWrite = "Chicken42"
      
      val writer = new RawFileWriter("file://ff")
      
      val outputStream = writer.write("rawFileWriterSpecs/useStream.raw")
      outputStream.write(dataToWrite.getBytes)
      outputStream.close();
      
      val reader = new RawFileReader("file://ff")
      
      val inputStream = reader.read("rawFileWriterSpecs/useStream.raw")
      val readDate = Source.fromInputStream(inputStream).mkString
      
      Assert.assertEquals(dataToWrite, readDate)
    }
  }
}