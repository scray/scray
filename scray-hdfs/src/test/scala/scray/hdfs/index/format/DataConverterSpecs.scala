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
package scray.hdfs.index.format

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.scalatest.WordSpec
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import org.junit.Assert
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.io.File
import scala.collection.mutable.MutableList

class DataConverterSpecs extends WordSpec with LazyLogging {
  "DataConverter " should {
    " create blob and idx records " in {

      val converter = new DataConverter
      
      val idx  = new IndexFile
      val data = new BlobFile
      
      for (i <- 0 to 10) {
        val key = s"key${i}".getBytes("UTF8")
        val value = s"val${i}".getBytes("UTF8")

        converter.addRecord(key, value, idx, data)
   
      }
      
      Assert.assertEquals("key0", new String(idx.getRecords.head.getKey))
      Assert.assertEquals("key1", new String(idx.getRecords.tail.head.getKey))
      Assert.assertEquals("key2", new String(idx.getRecords.tail.tail.head.getKey))
      
      Assert.assertEquals(0, idx.getRecords.head.getStartPosition)
      Assert.assertEquals(16, idx.getRecords.tail.head.getStartPosition)
      Assert.assertEquals(32, idx.getRecords.tail.tail.head.getStartPosition)
    }
    " create blob and idx records and wirite records to file " in {

      val converter = new DataConverter
      val idx  = new IndexFile
      val data = new BlobFile

      for (i <- 0 to 10000) {
        val key = s"key${i}".getBytes("UTF8")
        val value = s"val${i}".getBytes("UTF8")
   
        converter.addRecord(key, value, idx, data)

      }
      
     
      val time = System.currentTimeMillis()
      
      data.writeBlobFile(s"target/tsilerrorblobs-${time}.blob")
      idx.writeIndexFile(s"target/tsilerrorblobs-${time}.idx")
    }
  }
}