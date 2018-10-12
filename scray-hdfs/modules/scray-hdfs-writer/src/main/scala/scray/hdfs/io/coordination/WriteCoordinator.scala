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

package scray.hdfs.io.coordination

import scray.hdfs.io.write.IHdfsWriterConstats

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
 * @param fileFormat format to store files e.g. ORC, SequenceFile
 * @param version paths are versioned to handle them independently e.g. for compactions
 * @param maxFileSize If maxFileSize is reached a new file will be used
 * @param maxNumberOfInserts	If maxNumberOfInserts is reached a new file will be used.
 */
case class WriteDestination(
    queryspace: String, 
    path: String, 
    fileFormat: IHdfsWriterConstats.SequenceKeyValueFormat, 
    version: Version = Version(0), 
    maxFileSize: Long = 512 * 1024 * 1024,
    maxNumberOfInserts: Int = Integer.MAX_VALUE
   ) 