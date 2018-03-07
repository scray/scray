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

package scray.hdfs.index.format.sequence

import org.scalatest.WordSpec
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.junit.Assert
import org.scalatest.Tag
import com.typesafe.scalalogging.LazyLogging
import scray.hdfs.index.format.sequence.types.IndexValue
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.nio.charset.StandardCharsets
import org.apache.commons.io.IOUtils
import scray.hdfs.index.format.sequence.types.BlobInputStream
import scala.io.Source

class SequenceFileWriterSpecs extends WordSpec with LazyLogging {

  object RequiresHDFS extends Tag("scray.hdfs.tags.RequiresHDFS")

  def getKey(v: Integer) = "id_" + v
  def getValue(v: Integer) = "data_" + v

  "SequenceFileWriter " should {
    " read idx" in {

      val writer = new SequenceFileWriter("target/SeqFilWriterTest")

      for (i <- 0 to 1000) {
        writer.insert(getKey(i), 100000, getValue(i).getBytes)
      }
      writer.close

      // Seek to sync-marker at byte 22497 and return next data element
      val reader = new IdxReader("target/SeqFilWriterTest.idx")

      Assert.assertEquals(reader.hasNext, true)
      Assert.assertEquals(reader.next.isDefined, true)
      Assert.assertEquals(reader.next.get.getUpdateTime, 100000)
    }
    " read all index entries " in {
      val numDate = 1000 // Number of test data

      val writer = new SequenceFileWriter("target/IdxReaderTest")

      for (i <- 0 to numDate) {
        writer.insert(getKey(i), i, getValue(i).getBytes)
      }
      writer.close

      // Seek to sync-marker at byte 22497 and return next data element
      val reader = new IdxReader("target/IdxReaderTest.idx")

      Assert.assertEquals(reader.hasNext, true)

      for (i <- 0 to numDate) {
        val data = reader.next()
        Assert.assertTrue(data.isDefined)
        Assert.assertEquals(getKey(i), data.get.getKey.toString())
        Assert.assertEquals(i, data.get.getUpdateTime)
      }
    }
    " get data for a given index entry " in {
      val conf = new Configuration
      val fs = FileSystem.getLocal(conf)

      val numDate = 1000 // Number of test data

      val writer = new SequenceFileWriter("target/IdxReaderTest")

      for (i <- 0 to numDate) {
        writer.insert(getKey(i), i, getValue(i).getBytes)
      }
      writer.close

      val idxReader = new IdxReader("target/IdxReaderTest.idx")
      val blobReader = new BlobFileReader("target/IdxReaderTest.blob")

      // Read whole index file and check if corresponding data exists
      Assert.assertEquals(idxReader.hasNext, true)

      for (i <- 0 to numDate) {
        val idx  = idxReader.next().get
        val data = blobReader.get(idx.getKey.toString(), idx.getPosition)

        Assert.assertTrue(data.isDefined)
        Assert.assertEquals(new String(data.get), getValue(i))
      }
    }
    " get data for a given index entry as input stream " in {
      val conf = new Configuration
      val fs = FileSystem.getLocal(conf)

      val numDate = 1000 // Number of test data

      val writer = new SequenceFileWriter("target/IdxReaderTest1")

      for (i <- 0 to numDate) {
        writer.insert(getKey(i), i, getValue(i).getBytes)
      }
      writer.close

      val idxReader = new IdxReader("target/IdxReaderTest1.idx")
      val blobReader = new BlobFileReader("target/IdxReaderTest1.blob")


      // Read whole index file and check if corresponding data exists
      Assert.assertEquals(idxReader.hasNext, true)

      for (i <- 0 to numDate) {
        val idx  = idxReader.next().get
        val stream = new BlobInputStream(blobReader, idx)

        Assert.assertEquals(Source.fromInputStream(stream).mkString, getValue(i))
      }
    }
    " read and write data as InputStream " in {
      
      val inputData: InputStream = new ByteArrayInputStream(getValue(542).getBytes);
     
      val writer = new SequenceFileWriter("target/IoStreamRWTest")
      writer.insert(getKey(124), System.currentTimeMillis(), inputData)
      writer.close
      
      val idxReader = new IdxReader("target/IoStreamRWTest.idx")
      val blobReader = new BlobFileReader("target/IoStreamRWTest.blob")

      val idx  = idxReader.next().get
      val stream = new BlobInputStream(blobReader, idx)

      Assert.assertEquals(getValue(542), Source.fromInputStream(stream).mkString)
    }

  }
}