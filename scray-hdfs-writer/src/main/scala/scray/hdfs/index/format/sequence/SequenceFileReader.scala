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

import scray.hdfs.index.format.Reader
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.annotation.tailrec
import org.apache.hadoop.io.BytesWritable

class SequenceFileReader(hdfsConf: Configuration, path: String, fs: Option[FileSystem]) extends scray.hdfs.index.format.Reader {
  
  val key = new Text();
  val reader = new SequenceFile.Reader(hdfsConf, Reader.file(new Path(path + ".dat")), Reader.bufferSize(4096));

  def select(key: String): Array[Byte] = {
    Array("".toByte)
  }
  
  def get(key: String, startPosition: Long): Array[Byte] = {
    reader.seek(startPosition)
    
    val key = new Text
    val value = new BytesWritable
    
    var syncSeen = false
    while(!syncSeen && reader.next(key, value)) {
      syncSeen = reader.syncSeen();
    }

    value.copyBytes() // TODO test performance
  }
}