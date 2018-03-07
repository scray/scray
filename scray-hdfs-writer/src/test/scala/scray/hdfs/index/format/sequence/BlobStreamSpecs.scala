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

import com.typesafe.scalalogging.LazyLogging
import scray.hdfs.index.format.sequence.types.Blob
import org.scalatest.WordSpec
import org.apache.hadoop.io.SequenceFile
import java.io.ByteArrayInputStream
import java.io.InputStream
import scray.hdfs.index.format.sequence.types.IndexValue
import scray.hdfs.index.format.sequence.types.BlobInputStream
import org.junit.Assert
import scala.io.Source
import com.google.common.io.ByteStreams

class BlobStreamSpecs extends WordSpec with LazyLogging {

  "BlobStream " should {
        " read data byte by byte " in {
          val testData = "abcdefghijklmnopqrstuvwxyz".getBytes
          val reader = new TestBlobFileReader(testData, 1)
    
          val indexValue = new IndexValue("k1", 26, 1, System.currentTimeMillis(), 0)
          val inputStream = new BlobInputStream(reader, indexValue)
    
          for (offset <- 0 to 25) {
            Assert.assertEquals(inputStream.read(), testData(offset))
          }
        }
        " fill array " in {
          val testData = "abcdefghijklmnopqrstuvwxyz".getBytes
          val reader = new TestBlobFileReader(testData, 1)
    
          val indexValue = new IndexValue("k1", 26, 1, System.currentTimeMillis(), 0)
          val inputStream = new BlobInputStream(reader, indexValue)
    
          val readData = new Array[Byte](26)
          inputStream.read(readData)
           
          Assert.assertEquals(new String(testData), new String(readData))
        }
        " read from stream with guava toByteArray method " in {
          val testData = "abcdefghijklmnopqrstuvwxyz".getBytes
          val reader = new TestBlobFileReader(testData, 1)
    
          val indexValue = new IndexValue("k1", 26, 1, System.currentTimeMillis(), 0)
          val inputStream = new BlobInputStream(reader, indexValue)
          
          val bytes = ByteStreams.toByteArray(inputStream);
          
          Assert.assertArrayEquals(testData, bytes)
        }
        " read with scala.io.Source (splitSize 1) " in {
          val testData = "abcdefghijklmnopqrstuvwxyz".getBytes
          val reader = new TestBlobFileReader(testData, 1)
    
          val indexValue = new IndexValue("k1", 26, 1, System.currentTimeMillis(), 0)
          val inputStream = new BlobInputStream(reader, indexValue)
    
          Assert.assertEquals(
              new String(testData), 
              Source.fromInputStream(inputStream).mkString
          )
        }
        " read with scala.io.Source (splitSize 2) " in {
          val testData = "abcdefghijklmnopqrstuvwxyz".getBytes
          val reader = new TestBlobFileReader(testData, 2)
    
          val indexValue = new IndexValue("k1", 26, 1, System.currentTimeMillis(), 0)
          val inputStream = new BlobInputStream(reader, indexValue)
    
          Assert.assertEquals(
              new String(testData), 
              Source.fromInputStream(inputStream).mkString
          )
        }
    " read with scala.io.Source (splitSize 3) " in {
      val testData = "abcdefghijklmnopqrstuvwxyz".getBytes
      val reader = new TestBlobFileReader(testData, 3)

      val indexValue = new IndexValue("k1", 26, 1, System.currentTimeMillis(), 0)
      val inputStream = new BlobInputStream(reader, indexValue)

      Assert.assertEquals(
        new String(testData),
        Source.fromInputStream(inputStream).mkString)
    }
        " read with scala.io.Source (splitSize 26) " in {
          val testData = "abcdefghijklmnopqrstuvwxyz".getBytes
          val reader = new TestBlobFileReader(testData, 26)
    
          val indexValue = new IndexValue("k1", 26, 1, System.currentTimeMillis(), 0)
          val inputStream = new BlobInputStream(reader, indexValue)
    
          Assert.assertEquals(
              new String(testData), 
              Source.fromInputStream(inputStream).mkString
          )
        }
  }

}

class TestBlobFileReader(data: Array[Byte], splitSize: Int) extends BlobFileReader(null.asInstanceOf[SequenceFile.Reader]) {

  override def getNextBlob(keyIn: String, offset: Int, startPosition: Long): Option[Tuple2[Long, Blob]] = {
    if ((offset * splitSize) < data.length) {

      // Of one split is not full return rest
      if (data.length < ((offset * splitSize) + splitSize)) {
        val dest = new Array[Byte](data.length - (offset * splitSize))
              println("A: " + (offset * splitSize) + "\t B: " + data.length + "\t C: " + splitSize + "\t D: " + dest.size + "\t E: " + (data.length - (offset * splitSize)))

        System.arraycopy(data,
          (offset * splitSize),
          dest,
          0,
          (data.length - (offset * splitSize)))
        Some(((data.length - (offset * splitSize)), new Blob(System.currentTimeMillis(), dest)))
      } else {
        val dest = new Array[Byte](splitSize)
        System.arraycopy(data,
          (offset * splitSize),
          dest,
          0,
          splitSize)
        Some(((startPosition + splitSize.toLong), new Blob(System.currentTimeMillis(), dest)))
      }
    } else {
      None
    }
  }

  override def getNextBlobAsStream(keyIn: String, offset: Int, startPosition: Long): Option[Tuple2[Long, InputStream]] = {
    val SPLIT_SIZE = 1
    val dest = Array[Byte](1)

    System.arraycopy(data,
      offset * SPLIT_SIZE,
      dest,
      0,
      SPLIT_SIZE)

    Some((startPosition + SPLIT_SIZE.toLong), new ByteArrayInputStream(dest))
  }
}