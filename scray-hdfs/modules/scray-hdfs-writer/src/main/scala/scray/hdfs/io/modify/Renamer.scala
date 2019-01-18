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

package scray.hdfs.io.modify

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.conf.Configuration
import scray.hdfs.io.write.ScrayListenableFuture
import scray.hdfs.io.write.WriteResult

class Renamer {

  def rename(source: String, destination: String, conf: Configuration = new Configuration()):  ScrayListenableFuture[WriteResult] = {
    try {
      
      val fs = FileSystem.get(URI.create(source), conf);

      fs.rename(new Path(source), new Path(destination));
      new ScrayListenableFuture(new WriteResult(true, s"File ${source} renamed to ${destination}", -1L))
    } catch {
      case e: Exception => {
        new ScrayListenableFuture(e)
      }
    }
  }

  def separateFilename(path: String): Tuple2[String, String] = {
    val splited = path.split("/")
    val filename = splited(splited.length - 1)
    (path.replace(filename, ""), filename)
  }
}