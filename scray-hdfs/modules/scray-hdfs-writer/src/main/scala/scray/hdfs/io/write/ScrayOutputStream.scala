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

package scray.hdfs.io.write

import java.io.OutputStream
import scray.hdfs.io.modify.Renamer

class ScrayOutputStream(stream: OutputStream, filename: String, nameAfterClose: String) extends OutputStream {

  def this(stream: OutputStream) =  {
    this(stream, null, null)
  }
  
  override def write(b: Int): Unit = this.synchronized {
    stream.write(b)
  }

  override def write(b: Array[Byte]): Unit = this.synchronized {
    stream.write(b)
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = this.synchronized {
    stream.write(b, off, len)
  }

  override def flush(): Unit = this.synchronized {
    stream.flush()
  }

  override def close(): Unit = this.synchronized {
      stream.close()
  }
}