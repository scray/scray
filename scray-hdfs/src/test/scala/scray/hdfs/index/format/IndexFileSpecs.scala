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
import java.io.FileInputStream
import java.io.BufferedInputStream
import java.io.DataInputStream
import java.io.File
import scala.collection.mutable.MutableList
import org.junit.Assert

class IndexFileSpecs extends WordSpec with LazyLogging {
  "Index file " should {
    "store key value pairs " in {
      val idxFile = new IndexFile

      val key1 = s"key1".getBytes("UTF8")

      idxFile.addRecord(new IndexFileRecord(key1, idxFile.getLastPosition), 42)

      Assert.assertEquals("key1", new String(idxFile.getRecords.head.getKey))
      Assert.assertEquals(0, idxFile.getRecords.head.getStartPosition)

      val key2 = s"key2".getBytes("UTF8")
      idxFile.addRecord(new IndexFileRecord(key2, idxFile.getLastPosition), 42)

      Assert.assertEquals("key2", new String(idxFile.getRecords.tail.head.getKey))
      Assert.assertEquals(54, idxFile.getRecords.tail.head.getStartPosition)
    }
    " read and write records to file " in {

      // Create and write data to file
      val idxFile = new IndexFile

      for (i <- 0 to 10) {
        val key = s"key${i}".getBytes("UTF8")
        val value = s"val${i}".getBytes("UTF8")

        idxFile.addRecord(IndexFileRecord(key, 0, value.length), 42)
      }
      idxFile.writeIndexFile("file://target/testIdxFile.idx")

      // Read data from file and check result
      val fileInputStream = new FileInputStream(new File("target/testIdxFile.idx"))
      val is = new DataInputStream(new BufferedInputStream(fileInputStream, 2000000))

      val indexFile = IndexFile.apply.getReader(is)

      Assert.assertTrue(indexFile.hasNextRecord)
      Assert.assertEquals("key0", new String(indexFile.getNextRecord.get.getKey))
      Assert.assertEquals(8, indexFile.getNextRecord.get.getStartPosition)
    }
  }
}