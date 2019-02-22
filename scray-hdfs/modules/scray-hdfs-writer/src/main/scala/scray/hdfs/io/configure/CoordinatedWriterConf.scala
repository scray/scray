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

package scray.hdfs.io.configure

import scray.hdfs.io.write.IHdfsWriterConstats
import java.util.Optional
import scray.hdfs.io.configure.WriteDestionationConstats.WriteMode

class CompactionState {}
case object NEW extends CompactionState
case object IsReadyForCompaction extends CompactionState
case object CompactionIsStarted extends CompactionState
case object IsCompacted extends CompactionState

case class Version(number: Int, compactionState: CompactionState = NEW) {
  def this(number: Int) = {
    this(number, NEW)
  }
}
/**
 * Parameters for WriteCoordinatr
 * 
 * @param id id of logical system
 * @param path path to folder
 * @param customFileName defines a file name. If not defined a random UUID will be used
 * @param fileFormat format to store files e.g. ORC, SequenceFile
 * @param version paths are versioned to handle them independently e.g. for compactions
 * @param maxFileSize If maxFileSize is reached a new file will be used
 * @param maxNumberOfInserts	If maxNumberOfInserts is reached a new file will be used.
 */
class WriteParameter(
    var queryspace: String,
    val user: String,
    val password: Array[Byte],
    var path: String,
    var fileNameCreator: Optional[FilenameCreator] = Optional.empty(),
    var fileFormat: IHdfsWriterConstats.SequenceKeyValueFormat, 
    var version: Version = Version(0),
    var writeVersioned: Boolean = false,
    var maxFileSize: Long = Long.MaxValue,
    var maxNumberOfInserts: Int = Integer.MAX_VALUE,
    var storeAsHiddenFileTillClosed: Boolean = false,
    var createScrayIndexFile: Boolean = false
   ){}
   
object WriteParameter {
  class Builder {
    var queryspace: String = null
    var user: String = System.getProperty("user.name")
    var password: Array[Byte] = new Array[Byte](0)
    var path: String = null
    var fileNameCreator: Optional[FilenameCreator] = Optional.empty()
    var fileFormat: IHdfsWriterConstats.SequenceKeyValueFormat = null
    var version: Version = Version(0)
    var writeVersioned: Boolean = false
    var maxFileSize: Long = Long.MaxValue
    var maxNumberOfInserts: Int = Integer.MAX_VALUE
    var timeLimit: Int = Integer.MAX_VALUE
    var writeMode: WriteMode = WriteMode.WriteBack
    var storeAsHiddenFileTillClosed: Boolean = false
    var createScrayIndexFile: Boolean = false
    
    def setQueryspace(queryspace: String): Builder = {
      this.queryspace = queryspace
      this
    }
    
    def setPath(path: String): Builder = {
      this.path = path
      this
    }

    def setFileNameCreator(fileName: FilenameCreator): Builder = {
      this.fileNameCreator = Optional.of(fileName);
      this     
    }

    def setUser(user: String): Builder = {
      this.user = user;
      this
    }
    
    def setPassword(password: Array[Byte]) = {
      this.password = password
      this
    }
        
    def setFileFormat(kvFormat: IHdfsWriterConstats.SequenceKeyValueFormat): Builder = {
      this.fileFormat = kvFormat
      this
    }
    
    def setVersion(version: Version): Builder = {
      this.version = version
      this
    }
    
    def setWriteVersioned(writeVersioned: Boolean): Builder = {
      this.writeVersioned = writeVersioned
      this
    }

    def setMaxFileSize(maxFileSize: Long): Builder = {
      this.maxFileSize = maxFileSize
      this
    }

    def setMaxNumberOfInserts(maxNumberOfInserts: Int): Builder = {
      this.maxNumberOfInserts = maxNumberOfInserts
      this
    }

    def setTimeLimit(seconds: Int): Builder = {
      this.timeLimit = seconds
      this
    }
    
    def setWriteMode(mode: WriteMode): Builder = {
      this.writeMode = mode 
      this
    }
    
    def setStoreAsHiddenFileTillClosed(storeAsHiddenFileTillClosed: Boolean): Builder = {
      this.storeAsHiddenFileTillClosed = storeAsHiddenFileTillClosed
      this
    }

    def setCreateScrayIndexFile(createScrayIndexFile: Boolean): Builder = {
      this.createScrayIndexFile = createScrayIndexFile
      this
    }

    def createConfiguration: WriteParameter = {
      new WriteParameter(
          queryspace,
          user,
          password,
          path,
          fileNameCreator,
          fileFormat,
          version,
          writeVersioned,
          maxFileSize, 
          maxNumberOfInserts,
          storeAsHiddenFileTillClosed,
          createScrayIndexFile)
    }
  }
}