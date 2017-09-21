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

package scray.hdfs.index.format.orc

import org.scalatest.WordSpec
import scray.hdfs.index.format.sequence.BlobFileReader
import org.junit.Assert
import com.typesafe.scalalogging.LazyLogging

class ORCFileWriterSpecs extends WordSpec with LazyLogging {
  
  def getKey(v: Integer) = "id_" + v
  def getValue(v: Integer) = "data_" + v
  
  "ORCFileWriter " should {
    " write and read data to file " in {
      val filename = "target/ORCFilWriterTest_" + System.currentTimeMillis() + ".orc" 
      val writer = new ORCFileWriter(filename)

      for (i <- 0 to 2000) {
        writer.insert(getKey(i), 100000, getValue(i).getBytes)
      }
      writer.close

      val reader = new ORCFileReader(filename)
      
      val data = reader.get(getKey(1))
      Assert.assertTrue(data.isDefined)
      Assert.assertEquals(getValue(1), new String(data.get))
      
      val data2 = reader.get(getKey(42))
      Assert.assertTrue(data2.isDefined)
      Assert.assertEquals(getValue(42), new String(data2.get))
      
      val data3 = reader.get(getKey(2000))
      Assert.assertTrue(data3.isDefined)
      Assert.assertEquals(getValue(2000), new String(data3.get))
    }  
  }
}