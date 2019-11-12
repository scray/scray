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

package scray.hdfs.io.index.format.sequence

import java.io.InputStream
import java.math.BigInteger

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.SequenceFile.Metadata
import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable

import com.typesafe.scalalogging.LazyLogging

import scray.hdfs.io.index.format.Writer
import scray.hdfs.io.index.format.sequence.mapping.SequenceKeyValuePair
import java.io.File
import org.apache.hadoop.security.UserGroupInformation
import java.security.PrivilegedAction

class SequenceFileWriter[IDXKEY <: Writable, IDXVALUE <: Writable, DATAKEY <: Writable, DATAVALUE <: Writable](path: String, val hdfsConf: Configuration, fs: Option[FileSystem], outTypeMapping: SequenceKeyValuePair[IDXKEY, IDXVALUE, DATAKEY, DATAVALUE], createIndex: Boolean, user: String, compressionType: String) extends scray.hdfs.io.index.format.Writer with LazyLogging {

  var dataWriter: SequenceFile.Writer = null; // scalastyle:off null
  var idxWriter: Option[SequenceFile.Writer] = None
  val remoteUser: UserGroupInformation = UserGroupInformation.createRemoteUser(user)

  if (getClass.getClassLoader != null) {
    hdfsConf.setClassLoader(getClass.getClassLoader)
  }

  var numberOfInserts: Int = 0

  def this(path: String, outTypeMapping: SequenceKeyValuePair[IDXKEY, IDXVALUE, DATAKEY, DATAVALUE], createIndex: Boolean, user: String, compressionType: String) = {
    this(path, new Configuration, None, outTypeMapping, createIndex, user, compressionType)
  }

  def this(path: String, hdfsConf: Configuration, outTypeMapping: SequenceKeyValuePair[IDXKEY, IDXVALUE, DATAKEY, DATAVALUE], createIndex: Boolean, user: String, compressionType: String) {
    this(path, hdfsConf, None, outTypeMapping, createIndex, user, compressionType)
  }

  private def initWriter(
    key:           Writable,
    value:         Writable,
    fs:            FileSystem,
    fileExtension: String): SequenceFile.Writer = {

    hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    hdfsConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
    hdfsConf.set("dfs.client.use.datanode.hostname", "true");

    var writer: SequenceFile.Writer = null;

    remoteUser.doAs(new PrivilegedAction[Unit] {
      def run(): Unit = {
        
        
        val writerOptions = Array[org.apache.hadoop.io.SequenceFile.Writer.Option](
            Writer.compression(SequenceFile.CompressionType.RECORD),
            Writer.valueClass(value.getClass())
            )
        

        writer = SequenceFile.createWriter(hdfsConf, Writer.file(new Path(path + fileExtension)),
          Writer.keyClass(key.getClass()),
          Writer.valueClass(value.getClass()),
          Writer.bufferSize(fs.getConf().getInt("io.file.buffer.size", 4096)),
          Writer.replication(fs.getDefaultReplication()),
          Writer.blockSize(536870912),
          Writer.compression(SequenceFile.CompressionType.valueOf(compressionType)),
          Writer.progressable(null),
          Writer.metadata(new Metadata()));
      }
    })
    writer
  }

  private def createWinutilsDummy(basepath: String) {
    // Create dummy file if real WINUTILS.EXE is not required.
    val dummyFile = new File(basepath + System.getProperty("file.separator") + "HADOOP_HOME");
    System.getProperties().put("hadoop.home.dir", dummyFile.getAbsolutePath());
    new File("./bin").mkdirs();
    new File("./bin/winutils.exe").createNewFile();
  }

  def flush() = {
    remoteUser.doAs(new PrivilegedAction[Unit] {
      def run(): Unit = {
        if (dataWriter != null) dataWriter.hflush()
        idxWriter.map(_.hflush())
      }
    })
  }

  override def insert(id: String, data: String): Long = {

    hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    hdfsConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

    if (dataWriter == null) { // scalastyle:off null
      if (createIndex) {
        dataWriter = initWriter(outTypeMapping.getDataKey("42"), outTypeMapping.getDataValue("".getBytes), fs.getOrElse(FileSystem.get(hdfsConf)), ".data.seq")
      } else {
        dataWriter = initWriter(outTypeMapping.getDataKey("42"), outTypeMapping.getDataValue("".getBytes), fs.getOrElse(FileSystem.get(hdfsConf)), "")
      }
    }

    if (this.createIndex && !idxWriter.isDefined) { // scalastyle:off null
      idxWriter = Some(initWriter(outTypeMapping.getIdxKey("42"), outTypeMapping.getIdxValue("42", 42L, 2L), fs.getOrElse(FileSystem.get(hdfsConf)), ".idx.seq"))
    }

    remoteUser.doAs(new PrivilegedAction[Unit] {
      def run(): Unit = {
        // Write idx
        idxWriter.map(_.append(outTypeMapping.getIdxKey(id), outTypeMapping.getIdxValue(id, System.currentTimeMillis(), dataWriter.getLength)))

        // Write data
        //dataWriter.append(outTypeMapping.getDataKey(id), outTypeMapping.getDataValue(data));
        dataWriter.append(outTypeMapping.getDataKey(id), outTypeMapping.getDataValue(data));

        numberOfInserts = numberOfInserts + 1
      }
    })
    dataWriter.getLength

  }

  override def insert(id: String, updateTime: Long, data: Array[Byte]): Long = {

    hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    hdfsConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

    if (dataWriter == null) { // scalastyle:off null
      if (createIndex) {
        dataWriter = initWriter(outTypeMapping.getDataKey("42"), outTypeMapping.getDataValue("".getBytes), fs.getOrElse(FileSystem.get(hdfsConf)), ".data.seq")
      } else {
        dataWriter = initWriter(outTypeMapping.getDataKey("42"), outTypeMapping.getDataValue("".getBytes), fs.getOrElse(FileSystem.get(hdfsConf)), "")
      }
    }

    if (this.createIndex && !idxWriter.isDefined) { // scalastyle:off null
      idxWriter = Some(initWriter(outTypeMapping.getIdxKey("42"), outTypeMapping.getIdxValue("42", 42L, 2L), fs.getOrElse(FileSystem.get(hdfsConf)), ".idx.seq"))
    }
    remoteUser.doAs(new PrivilegedAction[Unit] {
      def run(): Unit = {
        // Write idx
        idxWriter.map(_.append(outTypeMapping.getIdxKey(id), outTypeMapping.getIdxValue(id, updateTime, dataWriter.getLength)))

        // Write data
        dataWriter.append(outTypeMapping.getDataKey(id), outTypeMapping.getDataValue(data));
        dataWriter.hflush()
        dataWriter.hsync()
      }
    })
    numberOfInserts = numberOfInserts + 1
    dataWriter.getLength
  }

  override def insert(id: String, updateTime: Long, data: InputStream, blobSplitSize: Int = 500 * 1024 * 1024): Long = {
    hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    hdfsConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

    if (dataWriter == null) { // scalastyle:off null
      if (createIndex) {
        dataWriter = initWriter(outTypeMapping.getDataKey("42"), outTypeMapping.getDataValue("".getBytes), fs.getOrElse(FileSystem.get(hdfsConf)), ".data.seq")
      } else {
        dataWriter = initWriter(outTypeMapping.getDataKey("42"), outTypeMapping.getDataValue("".getBytes), fs.getOrElse(FileSystem.get(hdfsConf)), "")
      }
    }

    if (this.createIndex && !idxWriter.isDefined) {
      idxWriter = Some(initWriter(outTypeMapping.getIdxKey("42"), outTypeMapping.getIdxValue("42", 42L, 2L), fs.getOrElse(FileSystem.get(hdfsConf)), ".idx.seq"))
    }

    if (blobSplitSize < 1) {
      logger.error(s"BlobSlitSize schould be at least 1byte. Used value ${blobSplitSize}")
    }

    val fileStartPossiton = dataWriter.getLength
    var writtenBytes = 0L // Number of written bytes
    var blobCounter = -1

    val buffer = new Array[Byte](blobSplitSize)
    var readDataLen = data.read(buffer)

    var reachMaxSizeBufferWrittenBytes = 0
    var reachMaxSizeBuffer = new Array[Byte](blobSplitSize)

    while (readDataLen != -1) {

      // Put files in buffer if spit size is not reached
      if ((reachMaxSizeBufferWrittenBytes + readDataLen) < blobSplitSize) {

        for (i <- 0 to (readDataLen - 1)) {
          reachMaxSizeBuffer(reachMaxSizeBufferWrittenBytes) = buffer(i)
          reachMaxSizeBufferWrittenBytes += 1
        }
      } else {
        blobCounter += 1
        logger.debug(s"Write next blob of size ${readDataLen} with offset nr ${blobCounter}.")

        val blob = outTypeMapping.getDataValue(reachMaxSizeBuffer, reachMaxSizeBufferWrittenBytes)
        remoteUser.doAs(new PrivilegedAction[Unit] {
          def run(): Unit = {
            dataWriter.append(outTypeMapping.getDataKey(id, blobCounter), blob)

            // Write idx
            idxWriter.map(_.append(new Text(id), outTypeMapping.getIdxValue(id, blobCounter, reachMaxSizeBufferWrittenBytes, updateTime, fileStartPossiton)))
          }
        })
        reachMaxSizeBuffer = new Array[Byte](0)
        reachMaxSizeBufferWrittenBytes = 0
      }
      readDataLen = data.read(buffer)
    }

    // Write missing data if inputstream terminated
    if (reachMaxSizeBufferWrittenBytes > 0) {
      blobCounter += 1
      val blob = outTypeMapping.getDataValue(reachMaxSizeBuffer, reachMaxSizeBufferWrittenBytes)
      remoteUser.doAs(new PrivilegedAction[Unit] {
        def run(): Unit = {
          dataWriter.append(outTypeMapping.getDataKey(id, blobCounter), blob)

          // Write idx
          idxWriter.map(_.append(outTypeMapping.getIdxKey(id), outTypeMapping.getIdxValue(id, blobCounter, reachMaxSizeBufferWrittenBytes, updateTime, fileStartPossiton)))
          reachMaxSizeBuffer = new Array[Byte](0)
          reachMaxSizeBufferWrittenBytes = 0
        }
      })
    }

    numberOfInserts = numberOfInserts + 1
    dataWriter.getLength + idxWriter.map(_.getLength).getOrElse(0L)
  }

  override def insert(id: String, updateTime: Long, data: InputStream, dataSize: BigInteger, blobSplitSize: Int): Long = {
    this.insert(id, updateTime, data, blobSplitSize)
  }

  //  def insert(idBlob: Tuple2[String, Blob]): Unit = {
  //    hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
  //    hdfsConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
  //
  //    val (id, blob) = idBlob
  //
  //    if (dataWriter == null) { // scalastyle:off null
  //      dataWriter = initWriter(new BlobKey, new Blob(), fs.getOrElse(FileSystem.get(hdfsConf)), ".blob")
  //    }
  //
  //    if (idxWriter == null) { // scalastyle:off null
  //      idxWriter = initWriter(new Text, idxValue, fs.getOrElse(FileSystem.get(hdfsConf)), ".idx")
  //    }
  //
  //    // Write idx
  //    idxWriter.append(new Text(id), new IndexValue(id, blob.getUpdateTime, dataWriter.getLength))
  //
  //    // Write data
  //    dataWriter.append(new Text(id), blob)
  //
  //    numberOfInserts = numberOfInserts + 1
  //  }

  def insert(id: String, updateTime: Long, data: String): Unit = {
    hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    hdfsConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

    if (dataWriter == null) { // scalastyle:off null
      if (createIndex) {
        dataWriter = initWriter(outTypeMapping.getDataKey("42"), outTypeMapping.getDataValue("".getBytes), fs.getOrElse(FileSystem.get(hdfsConf)), ".data.seq")
      } else {
        dataWriter = initWriter(outTypeMapping.getDataKey("42"), outTypeMapping.getDataValue("".getBytes), fs.getOrElse(FileSystem.get(hdfsConf)), "")
      }
    }

    if (this.createIndex && !idxWriter.isDefined) { // scalastyle:off null
      remoteUser.doAs(new PrivilegedAction[Unit] {
        def run(): Unit = {
          idxWriter = Some(initWriter(outTypeMapping.getIdxKey("42"), outTypeMapping.getIdxValue("42", 42L, 2L), fs.getOrElse(FileSystem.get(hdfsConf)), ".idx.seq"))
        }
      })
    }

    // Write idx
    remoteUser.doAs(new PrivilegedAction[Unit] {
      def run(): Unit = {
        idxWriter.map(_.append(outTypeMapping.getIdxKey(id), outTypeMapping.getIdxValue(id, updateTime, dataWriter.getLength)))
      }
    })

    // Write data
    remoteUser.doAs(new PrivilegedAction[Unit] {
      def run(): Unit = {
        dataWriter.append(outTypeMapping.getDataKey(id), outTypeMapping.getDataValue(data))
      }
    })
    numberOfInserts = numberOfInserts + 1
  }

  def getBytesWritten: Long = {
    if (dataWriter == null) { // scalastyle:off null
      0
    } else {
      dataWriter.getLength
    }
  }

  def getNumberOfInserts: Int = {
    numberOfInserts
  }

  def getPath: String = {
    this.path
  }

  def close: Unit = {
    remoteUser.doAs(new PrivilegedAction[Unit] {
      def run(): Unit = {
        if(dataWriter != null) {
          dataWriter.hflush()
          IOUtils.closeStream(dataWriter)
        }

        idxWriter.map(idxWriter => idxWriter.hflush())
        idxWriter.map(idxWriter => IOUtils.closeStream(idxWriter))
      }
    })
    varIsClosed = true
  }
}
