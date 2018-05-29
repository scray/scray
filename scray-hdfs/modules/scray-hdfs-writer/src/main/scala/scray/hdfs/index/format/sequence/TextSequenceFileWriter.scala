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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.SequenceFile.Metadata
import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.io.Text
import com.typesafe.scalalogging.LazyLogging
import java.io.InputStream
import scala.io.Source

class TextSequenceFileWriter (path: String, hdfsConf: Configuration, fs: Option[FileSystem]) extends LazyLogging {

  var dataWriter: SequenceFile.Writer = null; // scalastyle:off null

  if(getClass.getClassLoader != null) {
    hdfsConf.setClassLoader(getClass.getClassLoader)
  }

  var numberOfInserts: Int = 0
 
  def this(path: String, hdfsUser: String) = {
    this(path, new Configuration, None)
    System.setProperty("HADOOP_USER_NAME", hdfsUser)
  }
  
  def this(path: String) = {
    this(path, new Configuration, None)
  }
  
  def this(path: String, hdfsConf: Configuration) {
    this(path, hdfsConf, None)
  }

  private def initWriter(
    key: Text,
    value: Text,
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
    
    val ff: SequenceFile = null
    
    if (dataWriter != null)dataWriter.hflush() 
  }
  
  /**
   * Create string from given InputStream and add it to sequence file.
   * Attention! Full InputStream is stored in memory
   */
  def insert(id: String, data: InputStream): Long = synchronized {
    val steamAsString = Source.fromInputStream(data).mkString
    
    this.insert(id, steamAsString)
  }
    
  def insert(id: String, data: String): Long = synchronized {

    hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    hdfsConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
    
    if (dataWriter == null) { // scalastyle:off null
      dataWriter = initWriter(new Text, new Text, fs.getOrElse(FileSystem.get(hdfsConf)), ".text.seq")
    }

    // Write data
    dataWriter.append(new Text(id), new Text(data));
    
    numberOfInserts = numberOfInserts + 1
    dataWriter.getLength
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
  
  def close: Unit = {
    IOUtils.closeStream(dataWriter);
  }

}