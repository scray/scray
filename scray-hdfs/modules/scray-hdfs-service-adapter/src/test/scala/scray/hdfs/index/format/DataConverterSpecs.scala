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

import org.scalatest.WordSpec
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import org.junit.Assert
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.io.File
import scala.collection.mutable.MutableList
import org.scalatest.FlatSpec
import org.scalatest.tagobjects.Slow
import org.scalatest.Tag
import com.typesafe.scalalogging.LazyLogging

object RequiresHDFS extends Tag("scray.hdfs.tags.RequiresHDFS")

class DataConverterSpecs extends WordSpec with LazyLogging {
  "DataConverter " should {
    " create blob and idx records " in {

      val converter = new DataConverter

      val idx = new IndexFile
      val data = new BlobFile

      for (i <- 0 to 9) {
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
    " create blob and idx records with different data sizes " in {

      val converter = new DataConverter

      val idx = new IndexFile
      val data = new BlobFile

      converter.addRecord(
        "key1".getBytes("UTF8"),
        "v".getBytes("UTF8"), idx, data)

      converter.addRecord(
        "key2".getBytes("UTF8"),
        "vv".getBytes("UTF8"), idx, data)

      converter.addRecord(
        "key3".getBytes("UTF8"),
        "vvv".getBytes("UTF8"), idx, data)

      Assert.assertEquals("key1", new String(idx.getRecords.head.getKey))
      Assert.assertEquals("key2", new String(idx.getRecords.tail.head.getKey))
      Assert.assertEquals("key3", new String(idx.getRecords.tail.tail.head.getKey))

      Assert.assertEquals(0, idx.getRecords.head.getStartPosition)
      Assert.assertEquals(13, idx.getRecords.tail.head.getStartPosition)
      Assert.assertEquals(27, idx.getRecords.tail.tail.head.getStartPosition)
    }
    " create blob and idx records and write records to file " in {

      val converter = new DataConverter
      val idx = new IndexFile
      val data = new BlobFile

      for (i <- 0 to 10000) {
        val key = s"key${i}".getBytes("UTF8")
        val value = s"val${i}".getBytes("UTF8")

        converter.addRecord(key, value, idx, data)

      }

      val time = System.currentTimeMillis()

      data.writeBlobFile(s"file://target/")
      idx.writeIndexFile(s"file://target/")
    }
    " create blob and idx records and write records to hdfs " taggedAs(RequiresHDFS) in {

      val converter = new DataConverter
      val idx = new IndexFile
      val data = new BlobFile

      for (i <- 0 to 10000) {
        val key = s"key${i}".getBytes("UTF8")
        val value = s"val${i}".getBytes("UTF8")

        converter.addRecord(key, value, idx, data)

      }

      val time = System.currentTimeMillis()

      data.writeBlobFile(s"hdfs://10.11.22.41:8020/bdq-blob/")
      idx.writeIndexFile(s"hdfs://10.11.22.41:8020/bdq-blob/")
    }
  }
}