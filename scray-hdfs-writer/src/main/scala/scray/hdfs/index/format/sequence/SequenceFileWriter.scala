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

class SequenceFileWriter(hdfsConf: Configuration, path: String, fs: Option[FileSystem]) extends scray.hdfs.index.format.Writer {

  var dataWriter: SequenceFile.Writer = null;
  var idxWriter:  SequenceFile.Writer = null;

  val key = new Text();
  val idxValue = new IndexValue // Block position in data file

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

  def insert(id: String, updateTime: Long, data: Array[Byte]) = {
    
    if(dataWriter == null) {
      dataWriter =  initWriter(key, new BytesWritable(), fs.getOrElse(FileSystem.get(hdfsConf)), ".dat")
    }
    
    if(idxWriter == null) {
      idxWriter = initWriter(key, idxValue, fs.getOrElse(FileSystem.get(hdfsConf)), ".idx")
    }
      
    // Write data
    key.set(id) 
    dataWriter.append(key, new BytesWritable(data));

    // Write idx
    idxWriter.append(key, new IndexValue(updateTime, dataWriter.getLength))
  }
  
  def close = {
    IOUtils.closeStream(dataWriter);
    IOUtils.closeStream(idxWriter);
  }

}