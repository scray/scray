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

import java.util.concurrent.atomic.AtomicInteger
import java.util.function.IntUnaryOperator
import scala.collection.mutable.HashMap
import com.typesafe.scalalogging.LazyLogging
import scala.collection.mutable.MutableList
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Buffer
import scray.hdfs.index.format.sequence.BinarySequenceFileWriter
import scray.hdfs.index.format.Writer
import java.util.UUID

class CompactionState {}
case object NEW extends CompactionState
case object IsReadyForCompaction extends CompactionState
case object CompactionIsStarted extends CompactionState
case object IsCompacted extends CompactionState

class ReadWriteCoordinatorImpl extends ReadCoordinator with WriteCoordinator with LazyLogging {

  private val readSource = new HashMap[String, Buffer[Version]]
  private val writeDestinations = new HashMap[WriteDestination, CoordinatedWriter]

  private val availableForCompaction = new HashMap[String, Buffer[Version]]
  private val activeCompactions = new HashMap[String, Buffer[Version]]

  def getWriter(metadata: WriteDestination): Writer = {
    writeDestinations.get(metadata) match {
      case Some(writer) => {
        if (writer.isClosed) {
          createNewWriter(metadata)
        } else {
          writer
        }
      }
      case None => createNewWriter(metadata)
    }
  }

  def getWriter(queryspace: String, path: String, fileFormat: IHdfsWriterConstats.FileFormat): Writer = {
    this.getWriter(WriteDestination(queryspace, path, fileFormat))
  }

  def getNewWriter(metadata: WriteDestination): Writer = {
    this.createNewWriter(metadata.queryspace, metadata.path, metadata.fileFormat)
  }

  private def createNewWriter(queryspace: String, path: String, fileFormat: IHdfsWriterConstats.FileFormat): Writer = {
    this.createNewWriter(WriteDestination(queryspace, path, fileFormat))
  }

  private def createNewWriter(metadata: WriteDestination): Writer = {
    logger.debug(s"Create new Writer ${metadata}")
    metadata.fileFormat match {
      case format: IHdfsWriterConstats.FileFormat => {
        val filePath = this.getPath(metadata.path, metadata.queryspace, metadata.version.number)
        val sWriter = new BinarySequenceFileWriter(filePath)
        writeDestinations.put(
          metadata,
          new CoordinatedWriter(sWriter, metadata.maxFileSize, this, metadata))
        this.getWriter(metadata)
      }
      case _ => new BinarySequenceFileWriter(s"${metadata.path}/${metadata.queryspace}/")
    }
  }
  


  def registerNewWriteDestination(queryspace: String) = {
    val e = new RuntimeException("")
    e.printStackTrace()
  }
  def switchToNextVersion(queryspace: String) = {
    val e = new RuntimeException("")
    e.printStackTrace()
  }
  def getReadSources(queryspace: String): Option[List[Version]] = {
    val e = new RuntimeException("")
    e.printStackTrace();
    None
  }

  private def getPath(basePath: String, queryspace: String, version: Int): String = {
    if (basePath.endsWith("/")) {
      s"${basePath}scray-data-${queryspace}-v${version}/${UUID.randomUUID()}"
    } else {
      s"${basePath}/scray-data-${queryspace}-v${version}/${UUID.randomUUID()}"
    }
  }
}

