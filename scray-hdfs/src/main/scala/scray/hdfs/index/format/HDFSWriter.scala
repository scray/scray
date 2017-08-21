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

import java.util.ArrayList
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.hadoop.fs.FSDataOutputStream
import java.util.concurrent.Callable

/**
 * Write index and data files to HDFS
 *
 * @param path Path to root directory of blob store
 * @param batchSize Size of buffer which will be flushed to HDFS if it is full.
 */
class HDFSWriter[T <: HasByteRepresentation](path: String, data: ArrayList[T]) extends Callable[Boolean] with LazyLogging {

  var outputStream: FSDataOutputStream = null;
  val config = new Configuration;
  var fileSystem: FileSystem = null
  init

  def init = {
    if (outputStream == null) {
      val conf = new Configuration;
      conf.set("fs.defaultFS", path);

      fileSystem = FileSystem.get(conf);
    }

//    if (!fileSystem.exists(new Path(path))) {
//      logger.debug(s"Create path ${path}")
//      fileSystem.mkdirs(new Path(path))
//    }
  }

  def writeIdx = {
    if (data.size() > 0) {
      val fileType = data.get(0) match {
        case t: BlobFileRecord  => ".blob"
        case t: IndexFileRecord => "idx"
      }
      outputStream = fileSystem.create(new Path(path + "/bdq-blob-" + System.currentTimeMillis() + fileType))

      logger.debug(s"Write ${data.size()} elemens of type ${fileType}")
      for (i <- 0 to (data.size() - 1)) {
        outputStream.write(data.get(i).getByteRepresentation)
      }

      outputStream.close()
    }
  }

  override def call: Boolean = {
    try {
      writeIdx
    } catch {
      case e: Exception => {
        logger.error(s"Error while writing to HDFS. Error ${e.getMessage}")
        e.printStackTrace()
        return false
      }
    }
    true
  }

//  def clearFolder = {
//    val conf = new Configuration;
//    fileSystem.delete(new Path(path), true)
//    this.init
//  }

  def close = {
    if (outputStream != null) {
      outputStream.close();
    } else {
      logger.debug("No stream to close");
    }
  }

}