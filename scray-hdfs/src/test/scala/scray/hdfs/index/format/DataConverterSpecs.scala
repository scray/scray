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
      val records = new MutableList[Tuple3[BlobFileRecord, IndexFileRecord, Long]] 
      var positon = 0L

      for (i <- 0 to 10) {
        val key = s"key${i}".getBytes("UTF8")
        val value = s"val${i}".getBytes("UTF8")

        val dataIdxRecord = converter.createDataAndIndexRecord(key, value, positon)
        positon = dataIdxRecord._3
        records += dataIdxRecord        
      }
      
      Assert.assertEquals("key0", new String(records.head._2.getKey))
      Assert.assertEquals("key1", new String(records.tail.head._2.getKey))
      Assert.assertEquals("key2", new String(records.tail.tail.head._2.getKey))
      
      Assert.assertEquals(0, records.head._2.getStartPosition)
      Assert.assertEquals(16, records.tail.head._2.getStartPosition)
      Assert.assertEquals(32, records.tail.tail.head._2.getStartPosition)
    }
    " create blob and idx records and wirite records to file " in {

      val converter = new DataConverter
      val records = new MutableList[Tuple3[BlobFileRecord, IndexFileRecord, Long]] 
      var positon = 0L

      for (i <- 0 to 10000) {
        val key = s"key${i}".getBytes("UTF8")
        val value = s"val${i}".getBytes("UTF8")

        val dataIdxRecord = converter.createDataAndIndexRecord(key, value, positon)
        positon = dataIdxRecord._3
        records += dataIdxRecord        
      }
      
      val blobRecords = records.foldLeft(new MutableList[BlobFileRecord])((acc, next) => {acc += next._1; acc})
      val idxRecords = records.foldLeft(new MutableList[IndexFileRecord])((acc, next) => {acc += next._2; acc})
      
      val time = System.currentTimeMillis()
      
      BlobFile.apply.writeBlobFile(s"target/tsilerrorblobs-${time}.blob", blobRecords.toList)
      IndexFile.apply.writeIndexFile(s"target/tsilerrorblobs-${time}.idx", idxRecords.toList)
    }
  }
}