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

import scray.hdfs.index.format.Writer
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import java.io.File
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.SequenceFile.Metadata
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.LongWritable
import scray.hdfs.index.format.sequence.types.IndexValue
import scray.hdfs.index.format.sequence.types.Blob

class SequenceFileWriter(path: String, hdfsConf: Configuration = new Configuration, fs: Option[FileSystem] = None) extends scray.hdfs.index.format.Writer {

  var dataWriter: SequenceFile.Writer = null; // scalastyle:off null
  var idxWriter:  SequenceFile.Writer = null; // scalastyle:off null

  val key = new Text();
  val idxValue = new IndexValue("k1", 42, 42) // Block position in data file

  private def initWriter(
      key:  Writable, 
      value: Writable, 
      fs: FileSystem,
      fileExtension: String
    ) = {

    val writer = SequenceFile.createWriter(hdfsConf, Writer.file(new Path(path + fileExtension )),
      Writer.keyClass(key.getClass()),
      Writer.valueClass(value.getClass()),
      Writer.bufferSize(fs.getConf().getInt("io.file.buffer.size", 4096)),
      Writer.replication(fs.getDefaultReplication()),
      Writer.blockSize(1073741824),
      Writer.compression(SequenceFile.CompressionType.NONE),
      Writer.progressable(null),
      Writer.metadata(new Metadata()));
    
    writer
  }

  override def insert(id: String, updateTime: Long, data: Array[Byte]): Unit = {

    if(dataWriter == null) { // scalastyle:off null
      dataWriter =  initWriter(key, new Blob(), fs.getOrElse(FileSystem.get(hdfsConf)), ".blob")
    }
    
    if(idxWriter == null) { // scalastyle:off null
      idxWriter = initWriter(key, idxValue, fs.getOrElse(FileSystem.get(hdfsConf)), ".idx")
    }
      
    // Write idx
    key.set(id) 
    idxWriter.append(key, new IndexValue(key.toString(), updateTime, dataWriter.getLength))
    
    // Write data
    dataWriter.append(key, new Blob(updateTime, data));
  }
  
  override def insert(idBlob: Tuple2[String, Blob]): Unit = {
    
    val (id, blob) = idBlob

    if(dataWriter == null) { // scalastyle:off null
      dataWriter =  initWriter(key, new Blob(), fs.getOrElse(FileSystem.get(hdfsConf)), ".blob")
    }
    
    if(idxWriter == null) { // scalastyle:off null
      idxWriter = initWriter(key, idxValue, fs.getOrElse(FileSystem.get(hdfsConf)), ".idx")
    }
      
    // Write idx
    key.set(id) 
    idxWriter.append(key, new IndexValue(key.toString(), blob.getUpdateTime, dataWriter.getLength))
    
    // Write data
    dataWriter.append(key, blob);
  }
  
  def insert(id: String, updateTime: Long, data: String): Unit = {

    if(dataWriter == null) { // scalastyle:off null
      dataWriter =  initWriter(key, new BytesWritable(), fs.getOrElse(FileSystem.get(hdfsConf)), ".blob")
    }
    
    if(idxWriter == null) { // scalastyle:off null
      idxWriter = initWriter(key, idxValue, fs.getOrElse(FileSystem.get(hdfsConf)), ".idx")
    }
      
    // Write idx
    key.set(id) 
    idxWriter.append(key, new IndexValue(key.toString(), updateTime, dataWriter.getLength))
    
    // Write data
    dataWriter.append(key,  new Blob(updateTime, data.getBytes))
  }
  
  
  def close: Unit = {
    IOUtils.closeStream(dataWriter);
    IOUtils.closeStream(idxWriter);
  }

}
