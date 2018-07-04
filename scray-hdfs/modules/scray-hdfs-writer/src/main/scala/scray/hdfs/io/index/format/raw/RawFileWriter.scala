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

package scray.hdfs.io.index.format.raw

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.fs.Path
import com.google.common.io.ByteStreams
import java.io.InputStream
import com.typesafe.scalalogging.LazyLogging
import java.io.OutputStream

class RawFileWriter(hdfsURL: String, hdfsConf: Configuration) extends LazyLogging {

  var dataWriter: FileSystem = null; // scalastyle:off null

  if (getClass.getClassLoader != null) {
    hdfsConf.setClassLoader(getClass.getClassLoader)
  }

  def this(hdfsUrl : String) = {
    this(hdfsUrl, new Configuration)
  }

  def initWriter() = {
    hdfsConf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName);
    hdfsConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName);
    hdfsConf.set("dfs.client.use.datanode.hostname", "true");
    hdfsConf.set("fs.defaultFS", hdfsURL)

    dataWriter = FileSystem.get(hdfsConf);
  }
  
  def write(fileName: String, data: InputStream) = synchronized {
    val hdfswritepath = new Path(fileName);

    if(dataWriter == null ) {
      logger.debug("Writer was not initialized. Will do it now")
      
      initWriter()
    }
    
    dataWriter.create(new Path(fileName))
    val hdfsOutputStream = dataWriter.create(hdfswritepath);

    ByteStreams.copy(data, hdfsOutputStream);
    data.close()
    hdfsOutputStream.hflush();
    hdfsOutputStream.hsync();  
  }
  
  def write(fileName: String): OutputStream = {
    if(dataWriter == null ) {
      logger.debug("Writer was not initialized. Will do it now")
      
      initWriter()
    }
        
    dataWriter.create(new Path(fileName))
  }
  
  def close = {
    dataWriter.close()
  }
}
