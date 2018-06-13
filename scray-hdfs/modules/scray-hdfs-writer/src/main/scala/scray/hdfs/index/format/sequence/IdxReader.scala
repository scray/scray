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

import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.SequenceFile.Reader
import scray.hdfs.index.format.sequence.types.IndexValue
import org.apache.hadoop.io.Text
import com.typesafe.scalalogging.LazyLogging

class IdxReader(path: String, hdfsConf: Configuration, fs: Option[FileSystem]) extends LazyLogging {

      if(getClass.getClassLoader == null) {
      hdfsConf.setClassLoader(getClass.getClassLoader)
    }
  
  logger.debug(s"Try to read from path ${path}")
  val reader: SequenceFile.Reader = new SequenceFile.Reader(hdfsConf, Reader.file(new Path(path)), Reader.bufferSize(4096));

  val key = new Text();
  val idxEntry = new IndexValue("k1", 42, 42)

  // Store state to
  private var hasNextWasCalled = false
  private var hasNextValue = false
  
  def this(path: String) = {
    this(path, new Configuration, None)
  }

  /**
   * Check if more elements exists
   */
  def hasNext: Boolean = synchronized {
    if (!hasNextWasCalled) { // Has next reads data from fs. For this reason we store the state
      hasNextValue = reader.next(key, idxEntry)
      hasNextWasCalled = true
      hasNextValue
    } else {
      hasNextValue
    }
  }

  /**
   * Return next IndexValue element.
   * If no elment exists return None
   */
  def next(): Option[IndexValue] = {
    // return read data or request new data 
    if (hasNextWasCalled) {
      if (hasNextValue) {
        hasNextWasCalled = false
        hasNextValue = false
        Some(idxEntry)
      } else {
        None
      }
    } else {
      hasNext
      next
    }
  }
  
  def close: Unit = {
    reader.close()
  }
}
