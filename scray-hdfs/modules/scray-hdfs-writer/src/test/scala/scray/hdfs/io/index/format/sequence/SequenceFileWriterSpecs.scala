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

package scray.hdfs.io.index.format.sequence

import org.scalatest.WordSpec
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.junit.Assert
import org.scalatest.Tag
import com.typesafe.scalalogging.LazyLogging
import scray.hdfs.io.index.format.sequence.types.IndexValue

import java.io.ByteArrayInputStream
import java.io.InputStream
import java.nio.charset.StandardCharsets
import org.apache.commons.io.IOUtils
import scray.hdfs.io.index.format.sequence.types.BlobInputStream
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputTextBytesWritable
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputBlob
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputBlob
import java.nio.file.Paths

class SequenceFileWriterSpecs extends WordSpec with LazyLogging {
  
  val pathToWinutils = classOf[SequenceFileWriterSpecs].getClassLoader.getResource("HADOOP_HOME/bin/winutils.exe");
  val hadoopHome = Paths.get(pathToWinutils.toURI()).toFile().toString().replace("\\bin\\winutils.exe", "")
  System.setProperty("hadoop.home.dir", hadoopHome)
  
  object RequiresHDFS extends Tag("scray.hdfs.tags.RequiresHDFS")

  def getKey(v: Integer) = "id_" + v
  def getValue(v: Integer) = "data_" + v

  "SequenceFileWriterTextBytes " should {
    " read idx" in {

      val writer = new SequenceFileWriter("target/SeqFilWriterTest", new OutputBlob, true)

      for (i <- 0 to 1000) {
        writer.insert(getKey(i), 100000, getValue(i).getBytes)
      }
      writer.close

      // Seek to sync-marker at byte 22497 and return next data element
      val reader = new IdxReader("target/SeqFilWriterTest.idx.seq", new OutputBlob)

      Assert.assertEquals(reader.hasNext, true)
      Assert.assertEquals(reader.next.isDefined, true)
      println(reader.next.get)
      Assert.assertEquals(reader.next.get.getUpdateTime, 100000)
    }
    " read all index entries " in {
      val numDate = 1000 // Number of test data

      val writer = new SequenceFileWriter("target/IdxReaderTest", new OutputBlob, true)

      for (i <- 0 to numDate) {
        writer.insert(getKey(i), i, getValue(i).getBytes)
      }
      writer.close

      // Seek to sync-marker at byte 22497 and return next data element
      val reader = new IdxReader("target/IdxReaderTest.idx.seq", new OutputBlob)

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

      val writer = new SequenceFileWriter("target/IdxReaderTest", new OutputBlob, true)

      for (i <- 0 to numDate) {
        writer.insert(getKey(i), i, getValue(i).getBytes)
      }
      writer.close

      val idxReader = new IdxReader("target/IdxReaderTest.idx.seq", new OutputBlob)
      val blobReader = new ValueFileReader("target/IdxReaderTest.data.seq", new OutputBlob)

      // Read whole index file and check if corresponding data exists
      Assert.assertEquals(idxReader.hasNext, true)

      for (i <- 0 to numDate) {
        val idx  = idxReader.next().get
        val data = blobReader.get(idx.getKey.toString(), idx.getPosition)

        Assert.assertTrue(data.isDefined)
        Assert.assertEquals(getValue(i), new String(data.get))
      }
    }
    " get data for a given index entry as input stream " in {
      val conf = new Configuration
      val fs = FileSystem.getLocal(conf)

      val numDate = 1000 // Number of test data

      val writer = new SequenceFileWriter("target/IdxReaderTest1", new OutputBlob, true)

      for (i <- 0 to numDate) {
        writer.insert(getKey(i), i, getValue(i).getBytes)
      }
      writer.close

      val idxReader = new IdxReader("target/IdxReaderTest1.idx.seq", new OutputBlob)
      val blobReader = new ValueFileReader("target/IdxReaderTest1.data.seq", new OutputBlob)


      // Read whole index file and check if corresponding data exists
      Assert.assertEquals(idxReader.hasNext, true)

      for (i <- 0 to numDate) {
        val idx  = idxReader.next().get
        val stream = new BlobInputStream(blobReader, idx)

        Assert.assertEquals(getValue(i), Source.fromInputStream(stream).mkString)
      }
    }
    " use read method multiple times " in {
      
      // Write data
      val inputData = new StringBuffer();
      for(i <- 0 to 4000) {
        inputData.append(s" ${i}")
      }
     
      val writer = new SequenceFileWriter("target/IoStreamRWTest", new OutputBlob, true)
      writer.insert(getKey(124), System.currentTimeMillis(), inputData.toString().getBytes)
      writer.close

      // Read data
      val idxReader = new IdxReader("target/IoStreamRWTest.idx.seq", new OutputBlob)
      val blobReader = new ValueFileReader("target/IoStreamRWTest.data.seq", new OutputBlob)

      val idx  = idxReader.next().get
      // Use setream
      val stream = new BlobInputStream(blobReader, idx)
      
      val buffer = new Array[Byte](5);

      var bytesRead = 0;
      val result =  new ArrayBuffer[Byte]
      
      while(bytesRead != -1) {
        bytesRead = stream.read(buffer)  
        if(bytesRead > 0) {
          buffer.map(result += _) 
        }
      }
      Assert.assertEquals(inputData.toString(), new String(result.toArray))
    }
    " read and write big files as InputStream " in {
      
      val inputData = new StringBuffer();
      for(i <- 0 to 4000) {
        inputData.append(s" ${i}")
      }
     
      val writer = new SequenceFileWriter("target/IoStreamRWTest", new OutputBlob, true)
      writer.insert(getKey(124), System.currentTimeMillis(), inputData.toString().getBytes)
      writer.close
      
      val idxReader = new IdxReader("target/IoStreamRWTest.idx.seq", new OutputBlob)
      val blobReader = new ValueFileReader("target/IoStreamRWTest.data.seq", new OutputBlob)

      val idx  = idxReader.next().get
      val stream = new BlobInputStream(blobReader, idx)

      Assert.assertEquals(inputData.toString(), Source.fromInputStream(stream).mkString)
    }

  }
}