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
import java.io.DataInputStream
import java.io.EOFException
import scalaz.std.stream
import java.io.DataOutputStream
import java.io.FileOutputStream
import scala.collection.mutable.MutableList
import java.net.MalformedURLException

class BlobFile extends LazyLogging {
  private val version = new Array[Byte](4)
  private val records = new MutableList[BlobFileRecord]


  def addRecord(record: BlobFileRecord) = {
    records += record
  }
  
  def getRecords: List[BlobFileRecord] = {
    records.toList
  }
  
  def getReader(stream: DataInputStream): BlobFileReader2 = {
    new BlobFileReader2(stream)
  }

  class BlobFileReader2(stream: DataInputStream) {
    val version = new Array[Byte](4)
    stream.read(version)

    var nextRecord: Option[BlobFileRecord] = None

    def hasNextRecord: Boolean = {
      try {
        nextRecord = Some(BlobFileRecord(stream))
        true
      } catch {
        case e: EOFException => false
        case e: Exception => {
          logger.error(s"Error while reding idx record ${e.getMessage}")
          false
        }
      }
    }

    def getNextRecord = {
      nextRecord
    }

    def getVersion = {
      version
    }
  }
  
  /**
   * @param path Path to blob folder
   */
  def writeBlobFile(path: String, flush: Boolean = false) {
    val version = Array[Byte](0, 0, 0, 1)
  
    if(path.startsWith("file://")) {
      val datOutput = new FileOutputStream(path.split("file://")(1) + "bdq-blob-" + System.currentTimeMillis() + ".blob" );
       
      datOutput.write(version);
      val recordsIter = records.iterator
      
      while (recordsIter.hasNext) {
        datOutput.write(recordsIter.next().getByteRepresentation)
      }

      datOutput.flush()
      datOutput.close()
      
    } else if(path.startsWith("hdfs://")) {
      val buffer = new Buffer(10000, path)

      records.map(record => {buffer.addValue(record, false)})
      if(flush) buffer.flush
      
    } else {
      throw new MalformedURLException(s"${path} (file:// or hdfs:// is supported) ")
    }
  }

  def getVersion = {
    version
  }
}

object BlobFile {
  def apply = {
    new  BlobFile
  }
}