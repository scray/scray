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

class BlobFile extends LazyLogging {
  val version = new Array[Byte](4)

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
  
  def writeBlobFile(path: String, recordsIn: List[BlobFileRecord]) {
    val version = Array[Byte](0, 0, 0, 1)
    
    val datOutput = new FileOutputStream(path);
       
      datOutput.write(version);
      val records = recordsIn.iterator
      
      while (records.hasNext) {
        datOutput.write(records.next().getByteRepresentation)
      }

      datOutput.flush()
      datOutput.close()
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