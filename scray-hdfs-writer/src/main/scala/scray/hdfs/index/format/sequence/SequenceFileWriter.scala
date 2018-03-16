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

import java.io.InputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.SequenceFile.Metadata
import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable

import com.typesafe.scalalogging.LazyLogging

import scray.hdfs.index.format.Writer
import scray.hdfs.index.format.sequence.types.Blob
import scray.hdfs.index.format.sequence.types.BlobKey
import scray.hdfs.index.format.sequence.types.IndexValue

class SequenceFileWriter(path: String, hdfsConf: Configuration, fs: Option[FileSystem]) extends scray.hdfs.index.format.Writer with LazyLogging {

  var dataWriter: SequenceFile.Writer = null; // scalastyle:off null
  var idxWriter: SequenceFile.Writer = null; // scalastyle:off null

  val idxValue = new IndexValue("k1", 42, 42) // Block position in data file

  def this(path: String) = {
    this(path, new Configuration, None)
  }
  
  def this(path: String, hdfsConf: Configuration) {
    this(path, hdfsConf, None)
  }

  private def initWriter(
    key: Writable,
    value: Writable,
    fs: FileSystem,
    fileExtension: String) = {

    hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    hdfsConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
    hdfsConf.set("dfs.client.use.datanode.hostname", "true");

    val writer = SequenceFile.createWriter(hdfsConf, Writer.file(new Path(path + fileExtension)),
      Writer.keyClass(key.getClass()),
      Writer.valueClass(value.getClass()),
      Writer.bufferSize(fs.getConf().getInt("io.file.buffer.size", 4096)),
      Writer.replication(fs.getDefaultReplication()),
      Writer.blockSize(536870912),
      Writer.compression(SequenceFile.CompressionType.NONE),
      Writer.progressable(null),
      Writer.metadata(new Metadata()));

    writer
  }

  def flush() = {
    if (dataWriter != null)dataWriter.hflush() 
    if (idxWriter != null)idxWriter.hflush() 
  }
  
  override def insert(id: String, updateTime: Long, data: Array[Byte]): Long = {

    hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    hdfsConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
    
    if (dataWriter == null) { // scalastyle:off null
      dataWriter = initWriter(new BlobKey, new Blob(), fs.getOrElse(FileSystem.get(hdfsConf)), ".blob")
    }

    if (idxWriter == null) { // scalastyle:off null
      idxWriter = initWriter(new Text(), idxValue, fs.getOrElse(FileSystem.get(hdfsConf)), ".idx")
    }

    // Write idx
    idxWriter.append(new Text(id), new IndexValue(id, updateTime, dataWriter.getLength))

    // Write data
    dataWriter.append(new BlobKey(id), new Blob(updateTime, data, data.length));
    
    dataWriter.getLength
  }

  override def insert(id: String, updateTime: Long, data: InputStream, blobSplitSize: Int = 5 * 1024 * 1024): Long = {
    hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    hdfsConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
    
    if (dataWriter == null) { // scalastyle:off null
      dataWriter = initWriter(new BlobKey, new Blob, fs.getOrElse(FileSystem.get(hdfsConf)), ".blob")
    }

    if (idxWriter == null) { // scalastyle:off null
      idxWriter = initWriter(new Text, idxValue, fs.getOrElse(FileSystem.get(hdfsConf)), ".idx")
    }

    val fileStartPossiton = dataWriter.getLength
    var writtenBytes = 0L // Number of written bytes 
    var blobCounter = -1

    val buffer = new Array[Byte](blobSplitSize)
    var readDataLen = data.read(buffer)

    while (readDataLen != -1) {
      blobCounter  += 1

      logger.debug(s"Write next blob of size ${readDataLen} with offset nr ${blobCounter}.")

      val blob = new Blob(System.currentTimeMillis(), buffer, readDataLen)
      dataWriter.append(new BlobKey(id, blobCounter), blob)

      readDataLen = data.read(buffer)
    }

    // Write idx
    idxWriter.append(new Text(id), new IndexValue(id, blobCounter, blobSplitSize, updateTime, fileStartPossiton))
    
    dataWriter.getLength + idxWriter.getLength
  }

  override def insert(idBlob: Tuple2[String, Blob]): Unit = {
    hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    hdfsConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

    val (id, blob) = idBlob

    if (dataWriter == null) { // scalastyle:off null
      dataWriter = initWriter(new BlobKey, new Blob(), fs.getOrElse(FileSystem.get(hdfsConf)), ".blob")
    }

    if (idxWriter == null) { // scalastyle:off null
      idxWriter = initWriter(new Text, idxValue, fs.getOrElse(FileSystem.get(hdfsConf)), ".idx")
    }

    // Write idx
    idxWriter.append(new Text(id), new IndexValue(id, blob.getUpdateTime, dataWriter.getLength))

    // Write data
    dataWriter.append(new Text(id), blob);
  }

  def insert(id: String, updateTime: Long, data: String): Unit = {
    hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    hdfsConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

    if (dataWriter == null) { // scalastyle:off null
      dataWriter = initWriter(new BlobKey, new BytesWritable(), fs.getOrElse(FileSystem.get(hdfsConf)), ".blob")
    }

    if (idxWriter == null) { // scalastyle:off null
      idxWriter = initWriter(new Text, idxValue, fs.getOrElse(FileSystem.get(hdfsConf)), ".idx")
    }

    // Write idx
    idxWriter.append(new Text(id), new IndexValue(id, updateTime, dataWriter.getLength))

    // Write data
    dataWriter.append(new BlobKey(id), new Blob(updateTime, data.getBytes, data.length()))
  }

  def getBytesWritten: Long = {
    if (dataWriter == null) { // scalastyle:off null
      0
    } else {
      dataWriter.getLength
    }
  }
  
  def close: Unit = {
    IOUtils.closeStream(dataWriter);
    IOUtils.closeStream(idxWriter);
  }

}
