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

package scray.hdfs.io.write

import java.io.InputStream
import java.io.OutputStream
import java.math.BigInteger
import java.util.UUID

import com.google.common.util.concurrent.ListenableFuture
import scray.hdfs.io.write.IHdfsWriterConstats.SequenceKeyValueFormat
import org.apache.hadoop.conf.Configuration

trait WriteService {
    /**
   * Create new writer instance on service side.
   * 
   * @return id to identify resource
   */
  def createWriter(path: String): UUID 
  def createWriter(path: String, format: SequenceKeyValueFormat): UUID
  def createWriter(path: String, format: SequenceKeyValueFormat, numberOpKeyValuePairs: Int): UUID
  
  
  def insert(resource: UUID, id: String, updateTime: Long, data: Array[Byte]):  ScrayListenableFuture[WriteResult]
  def insert(resource: UUID, id: String, updateTime: Long, data: InputStream, blobSplitSize: Int = 5 * 2048):  ScrayListenableFuture[WriteResult]
  def insert(resource: UUID, id: String, updateTime: Long, data: InputStream, dataSize: BigInteger, blobSplitSize: Int):  ScrayListenableFuture[WriteResult]
  
  def writeRawFile(path: String, data: InputStream): ScrayListenableFuture[WriteResult]
  /**
   * @param writeAndRename A dot will be set at the first character of the filename while writing. File will be renamed after stream was closed.
   */
  def writeRawFile(path: String): ScrayOutputStream
  
  def rename(source: String, destination: String): ScrayListenableFuture[WriteResult]

  def close(resource: UUID)
  def isClosed(resource: UUID): ScrayListenableFuture[WriteResult]
}