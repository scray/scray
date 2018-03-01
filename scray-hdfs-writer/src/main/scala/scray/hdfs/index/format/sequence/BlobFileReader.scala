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

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.annotation.tailrec
import org.apache.hadoop.io.BytesWritable
import scray.hdfs.index.format.sequence.types.Blob
import java.io.InputStream
import scray.hdfs.index.format.sequence.types.BlobKey
import java.io.ByteArrayInputStream

class BlobFileReader(path: String, hdfsConf: Configuration = new Configuration, fs: Option[FileSystem] = None) {

  private val key = new Text();
  private val reader = new SequenceFile.Reader(hdfsConf, Reader.file(new Path(path)), Reader.bufferSize(4096));
  val idxEntry = new Blob
  
  def this(path: String) = {
    this(path, new Configuration,  None)
  }
  
  def select(key: String): Array[Byte] = {
    Array("".toByte)
  }
  
  def get(keyIn: String, startPosition: Long): Option[Array[Byte]] = {
    getBlob(keyIn, 0, startPosition).map(_.getData)
  }
  
  def getBlobAsStream(keyIn: String, startPosition: Long): Option[InputStream] = {
    reader.seek(startPosition)

    val key = new BlobKey()
    val value = new Blob
    var valueFound = false
        
    var syncSeen = false
    while(!syncSeen && !valueFound && reader.next(key, value)) {
      syncSeen = reader.syncSeen();

      if(keyIn.equals(key.getId)) {
        valueFound = true
      }
    }

    if(valueFound) {
      valueFound = false
      Some(new ByteArrayInputStream(value.getData)) // FIXME Create continous stream over all offset s...
    } else {
      None
    }
  }
  
  def getBlob(keyIn: String, offset: Int, startPosition: Long): Option[Blob] = {
    reader.seek(startPosition)

    val key = new BlobKey()
    val value = new Blob
    var valueFound = false
    
    var syncSeen = false
    while(!syncSeen && !valueFound && reader.next(key, value)) {

      syncSeen = reader.syncSeen();

      if(keyIn.equals(key.getId) && offset == key.getOffset) {
        valueFound = true
      }
    }

    if(valueFound) {
      valueFound = false
      Some(value) // TODO test performance
    } else {
      None
    }
  }
  
  def getNextBlob(keyIn: String, offset: Int, startPosition: Long): Option[Tuple2[Long, Blob]] = {
    reader.seek(startPosition)

    val key = new BlobKey()
    val value = new Blob
    var valueFound = false
    
    var syncSeen = false
    while(!syncSeen && !valueFound && reader.next(key, value)) {

    syncSeen = reader.syncSeen();

      if(keyIn.equals(key.getId) && offset == key.getOffset) {
        valueFound = true
      }
    }

    if(valueFound) {
      valueFound = false
      Some(reader.getPosition, value) // TODO test performance
    } else {
      None
    }
  }
  
  def printBlobKeys(startPosition: Long): Unit = {
    reader.seek(startPosition)

    val key = new BlobKey()
    val value = new Blob

    while(reader.next(key, value)) {
      reader.getPosition
      println(s"Key: ${key}, possition: ${reader.getPosition}")
    }
  }
  
  def close = {
    reader.close()
  }
}
