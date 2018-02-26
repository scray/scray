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

package scray.hdfs.index.format.sequence.types

import java.io.InputStream
import java.io.IOException
import org.apache.hadoop.io.SequenceFile

class BlobInputStream(reader: SequenceFile.Reader, numSplits: Int, splitSize: Int) extends InputStream {
  var currentSplit = 0
  var possitionInBuffer = 0
  var dataBuffer: Array[Byte] = null

  
  override def read: Int = {
    if(possitionInBuffer <= dataBuffer.size) {
        possitionInBuffer = possitionInBuffer + 1
        dataBuffer(possitionInBuffer)
    } else {
        this.updateBuffer
        this.read
    }
  }
  
  override def read(b: Array[Byte]): Int = {
    this.read(b, 0, b.length)
  }
  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    
    0
  }
  
  override def skip(n: Long): Long = {
    // FIXME 
    n
  }
  
  override def available: Int = {
    0
  }
  
  override def close = {
    reader.close
  }
  override def mark(readlimit: Int) = {
    
  }
  
  override def reset = {
    throw new IOException()
  }
  
  override def markSupported: Boolean = {
    false
  }
  
  private def updateBuffer() = {
    
  }
}