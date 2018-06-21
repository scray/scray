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

package scray.hdfs.index.format.raw

import java.io.InputStream
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

class RawFileReader(hdfsURL: String, hdfsConf: Configuration) extends LazyLogging {
  
   var dataReader: FileSystem = null; // scalastyle:off null

   def this(hdfsURL: String) {
       this(hdfsURL, new Configuration)
   }
   
   def initReader() = {
    hdfsConf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName);
    hdfsConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName);
    hdfsConf.set("dfs.client.use.datanode.hostname", "true");
    hdfsConf.set("fs.defaultFS", hdfsURL)

    dataReader = FileSystem.get(hdfsConf);
  }
  
  def read(path: String): InputStream = {
   if(dataReader == null ) {
      logger.debug("Writer was not initialized. Will do it now")
      initReader()
    }
   
    dataReader.open(new Path(path))
  }
}