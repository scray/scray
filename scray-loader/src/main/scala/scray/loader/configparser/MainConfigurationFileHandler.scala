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
package scray.loader.configparser

import org.apache.hadoop.fs.Path
import scala.util.Try
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.commons.io.IOUtils
import java.net.URL
import scala.collection.mutable.HashMap
import scala.util.Failure
import scala.collection.mutable.ArrayBuffer
import com.typesafe.scalalogging.slf4j.LazyLogging
import java.io.File
import java.io.FilenameFilter
import java.io.IOException
import java.io.FileInputStream

/**
 * handles reading scray config files from external sources.
 */
// scalastyle:on multiple.string.literals
object MainConfigurationFileHandler extends LazyLogging {
  
  type UpdateCallback = (String, Option[(ScrayQueryspaceConfiguration, Long)]) => Unit
  
  /**
   * We ignore reload for now, which is controlled externally.
   */
  def readMainConfig(url: String): Try[ScrayConfiguration] = {
    if(url.substring(0, HDFS_SCHEMA_LENGTH).equalsIgnoreCase(HDFS_SCHEMA)) {
      // if the url starts with hdfs we read from HDFS
      try {
        val path = new Path(url)
        val fs = FileSystem.get(new Configuration())
        logger.info("Reading main configuration file " + url)
        ScrayConfigurationParser.parse(QueryspaceConfigurationFileHandler.readFileFromHDFS(path, fs), true)
      } catch {
        case e: Exception => Failure(e)
      }
    } else {
      // if the URL contains :// we expect this to be a URL to a file containing scray queryspace information
      if(url.contains(SCHEMA_SEPARATOR)) {
        // we could install an URL-Stream-handler, but this might require command line parameters, so we handle this hard-wired
        if(url.startsWith(RESOURCE_SCHEMA)) {
          logger.info("Reading main configuration from classpath " + url) 
          ScrayConfigurationParser.parseResource(url.stripPrefix(RESOURCE_SCHEMA), true)            
        } else {
          val fileURL = new URL(url)
          logger.info("Reading main configuration file " + url) 
          ScrayConfigurationParser.parse(IOUtils.toString(fileURL.openStream()), true)
        }
      } else {
        // this is either a regular file or a directory...
        val file = new File(url)
        if(file.exists() && file.isFile()) {
          logger.info("Read main configuration file " + url) 
          ScrayConfigurationParser.parse(IOUtils.toString(new FileInputStream(file)), true)
        } else {
          val msg = "Could not read queryspace configuration file " + url
          logger.warn(msg)
          Failure(new IOException(msg))
        }
      }
    }
  }
}
// scalastyle:off multiple.string.literals
