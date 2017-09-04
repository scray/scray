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

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.annotation.tailrec
import org.apache.hadoop.io.BytesWritable

class BlobFileReader(path: String, hdfsConf: Configuration = new Configuration, fs: Option[FileSystem] = None) {

  private val key = new Text();
  private val reader = new SequenceFile.Reader(hdfsConf, Reader.file(new Path(path)), Reader.bufferSize(4096));

  def select(key: String): Array[Byte] = {
    Array("".toByte)
  }
  
  def get(keyIn: String, startPosition: Long): Option[Array[Byte]] = {
    reader.seek(startPosition)

    val key = new Text
    val value = new BytesWritable
    var valueFound = false
    
    var syncSeen = false
    while(!syncSeen && !valueFound && reader.next(key, value)) {
      syncSeen = reader.syncSeen();

      if(keyIn.equals(key.toString())) {
        valueFound = true
      }
    }

    if(valueFound) {
      valueFound = false
      Some(value.copyBytes()) // TODO test performance
    } else {
      None
    }
  }
  
  def close {
    reader.close()
  }
}
