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

import java.io.DataInputStream
import java.io.EOFException
import java.io.FileOutputStream

import scala.collection.mutable.MutableList

import com.typesafe.scalalogging.slf4j.LazyLogging

class IndexFile extends LazyLogging {
  private val version = Array[Byte](0, 0, 0, 1)
  private var lastPosition = 0L
  private val records: MutableList[IndexFileRecord] = new MutableList[IndexFileRecord]
  
  
  def addRecord(record: IndexFileRecord) {
    lastPosition = lastPosition + 
           8 + // Bytes for key length and blob length
           record.getKey.length 
     
    records += record       
  }
  
  def getRecords: List[IndexFileRecord] = {
    records.toList
  }

  def getReader(stream: DataInputStream) = {
    new IndexFileReader2(stream)
  }

  class IndexFileReader2(stream: DataInputStream) {
    val version = new Array[Byte](4)
    stream.read(version)

    var nextRecord: Option[IndexFileRecord] = None

    def hasNextRecord: Boolean = {
      try {
        nextRecord = Some(IndexFileRecord(stream))
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
  
  def writeIndexFile(path: String) = {
      val datOutput = new FileOutputStream(path);
       
      datOutput.write(version);
      val recordsIter = records.iterator
      
      while (recordsIter.hasNext) {
        datOutput.write(recordsIter.next().getByteRepresentation)
      }

      datOutput.flush()
      datOutput.close()
    }

  def getVersion = {
    version
  }
  
  def getLastPosition: Long = {
    lastPosition
  }
}

object IndexFile {
  def apply = {
    new IndexFile
  }
}