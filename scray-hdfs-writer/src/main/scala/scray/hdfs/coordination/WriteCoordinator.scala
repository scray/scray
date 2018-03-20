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

package scray.hdfs.coordination

import scala.collection.mutable.HashMap
import scray.hdfs.index.format.Writer


case class Version(number: Int, compactionState: CompactionState = NEW) 
case class WriteDestination(
    queryspace: String, 
    path: String, 
    fileFormat: IHdfsWriterConstats.FileFormat, 
    version: Version = Version(0), 
    maxFileSize: Long = 512 * 1024 * 1024,
    maxNumberOfInserts: Int = Integer.MAX_VALUE)

trait WriteCoordinator {

  /**
   * Destination where new data should be written.
   */
  def getWriter(queryspace: String, path: String, fileFormat: IHdfsWriterConstats.FileFormat): Writer
  def getWriter(metadata: WriteDestination): Writer
  def getNewWriter(metadata: WriteDestination): Writer

}