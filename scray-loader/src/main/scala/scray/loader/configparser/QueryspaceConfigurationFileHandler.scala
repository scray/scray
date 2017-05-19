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
object QueryspaceConfigurationFileHandler extends LazyLogging {
  
  type UpdateCallback = (String, Option[(ScrayQueryspaceConfiguration, Long, Option[Long])]) => Unit
  
  val WITH_QS = " with queryspace "
  val READ_FILE = "Read queryspace configuration file "
  
  /**
   * reads in all the queryspace configurations and updates the configurations if necessary
   */
  def performQueryspaceUpdate(newConfig: ScrayConfiguration,
      oldqueryspaces: HashMap[String, (Long, ScrayQueryspaceConfiguration)],
      callbacks: Seq[UpdateCallback]): Unit = {
    val configs = scanDirectories(newConfig)
    val knownQS = new ArrayBuffer[String]
    configs.filter(_.isSuccess).foreach { scannedFiles =>
      knownQS += scannedFiles.get.name
      val oldQs = oldqueryspaces.get(scannedFiles.get.name) 
      if(oldQs.isEmpty) {
        // new queryspace registered... notify
        callbacks.foreach { _(scannedFiles.get.name, Some((scannedFiles.get.queryspaceConfig, scannedFiles.get.version, None))) }
        oldqueryspaces.update(scannedFiles.get.name, (scannedFiles.get.version, scannedFiles.get.queryspaceConfig))
      } else {
        // new version? -> notify
        if(oldQs.get._1 != scannedFiles.get.version) {
          logger.debug(s"New version of queryspace config file: previous version was ${oldQs.get._1} new is ${scannedFiles.get.version}, content is ${scannedFiles.get.queryspaceConfig}")
          callbacks.foreach { _(scannedFiles.get.name, Some((scannedFiles.get.queryspaceConfig, scannedFiles.get.version, Some(oldQs.get._1)))) }
          oldqueryspaces.update(scannedFiles.get.name, (scannedFiles.get.version, scannedFiles.get.queryspaceConfig))
        }
      }
    }
    // the queryspaces which were there, but aren't any more, must be notified
    (oldqueryspaces.keySet -- knownQS).foreach { qs => 
      callbacks.foreach { _(qs, None) }
      oldqueryspaces.remove(qs)
    } 
  }
  
  /**
   * reads a file into a String directly from HDFS
   */
  def readFileFromHDFS(filePath: Path, fs: FileSystem): String = {
    val istream = fs.open(filePath)
    try {
      IOUtils.toString(istream)
    } finally {
      istream.close
    }
  }
  
  private def handleHDFS(url: ScrayQueryspaceConfigurationURL, config: ScrayConfiguration): 
      Seq[Try[ScannedQueryspaceConfigfiles]] = {
    try {
      val path = new Path(url.url)
      val fs = FileSystem.get(new Configuration())
      if(fs.getFileStatus(path).isDir()) {
        logger.info("Reading directory for HDFS-URL " + url.url + ", scanning for *" + SCRAY_QUERYSPACE_CONFIG_ENDING + "...")
        // read complete directory
        val stati = fs.listStatus(path)
        stati.filter(_.getPath().getName().endsWith(SCRAY_QUERYSPACE_CONFIG_ENDING)).map { status => Try {
          val parsedFile = ScrayQueryspaceConfigurationParser.parse(readFileFromHDFS(status.getPath, fs), config, true)
          logger.info(READ_FILE + status.getPath + WITH_QS + parsedFile.get.name + "and version " + parsedFile.get.version) 
          ScannedQueryspaceConfigfiles(status.getPath.toString(), parsedFile.get.name, parsedFile.get.version, parsedFile.get)
        }}.toSeq
      } else {
        Seq(Try {
          val parsedFile = ScrayQueryspaceConfigurationParser.parse(readFileFromHDFS(path, fs), config, true)
          logger.info(READ_FILE + url.url + WITH_QS + parsedFile.get.name + "and version " + parsedFile.get.version)
          ScannedQueryspaceConfigfiles(url.url, parsedFile.get.name, parsedFile.get.version, parsedFile.get)
        })
      }
    } catch {
      case e: Exception => Seq(Failure(e))
    }  
  }
  
  private def handleURL(url: ScrayQueryspaceConfigurationURL, config: ScrayConfiguration): 
      Seq[Try[ScannedQueryspaceConfigfiles]] = {
    // we could install an URL-Stream-handler, but this might require command line parameters, so we handle this hard-wired
    if(url.url.startsWith(RESOURCE_SCHEMA)) {
      Seq(Try {
        val parsedFile = ScrayQueryspaceConfigurationParser.parseResource(url.url.stripPrefix(RESOURCE_SCHEMA), config, true)
        logger.warn("###" + parsedFile)
        logger.info("Read queryspace configuration from classpath " + url.url + WITH_QS + parsedFile.get.name) 
        ScannedQueryspaceConfigfiles(url.url, parsedFile.get.name, parsedFile.get.version, parsedFile.get)            
      })
    } else {
      Seq(Try {
        val fileURL = new URL(url.url)
        val parsedFile = ScrayQueryspaceConfigurationParser.parse(IOUtils.toString(fileURL.openStream()), config, true)
        logger.info(READ_FILE + url.url + WITH_QS + parsedFile.get.name + "and version " + parsedFile.get.version) 
        ScannedQueryspaceConfigfiles(url.url, parsedFile.get.name, parsedFile.get.version, parsedFile.get)
      })
    }
  }
  
  private def handleDiskFiles(url: ScrayQueryspaceConfigurationURL, config: ScrayConfiguration): 
      Seq[Try[ScannedQueryspaceConfigfiles]] = {
    val file = new File(url.url)
    if(file.exists()) {
      if(file.isDirectory()) {
        logger.info(s"Reading directory ${url.url}, scanning for *" + SCRAY_QUERYSPACE_CONFIG_ENDING + "...")
        val files = file.listFiles(new FilenameFilter() { 
          override def accept(dir: File, name: String): Boolean = name.endsWith(SCRAY_QUERYSPACE_CONFIG_ENDING) })
        files.map { conffile => Try {
          val parsedFile = ScrayQueryspaceConfigurationParser.parse(IOUtils.toString(new FileInputStream(conffile)), config, true)
          logger.info(READ_FILE + conffile.toString() + WITH_QS + parsedFile.get.name  + "and version " + parsedFile.get.version) 
          ScannedQueryspaceConfigfiles(conffile.toString(), parsedFile.get.name, parsedFile.get.version, parsedFile.get)
        }}.toSeq
      } else {
        Seq(Try {
          val parsedFile = ScrayQueryspaceConfigurationParser.parse(IOUtils.toString(new FileInputStream(file)), config, true)
          logger.info(READ_FILE + url.url + WITH_QS + parsedFile.get.name + "and version " + parsedFile.get.version) 
          ScannedQueryspaceConfigfiles(url.url, parsedFile.get.name, parsedFile.get.version, parsedFile.get)
        })
      }
    } else {
      val msg = "Could not read queryspace configuration file " + url.url
      logger.warn(msg)
      Seq(Failure(new IOException(msg)))
    }    
  }
  
  /**
   * Scan directories and files for Queryspaces.
   * We ignore reload for now, which is controlled externally.
   */
  def scanDirectories(config: ScrayConfiguration): Seq[Try[ScannedQueryspaceConfigfiles]] = {
    config.urls.map { url =>
      if(url.url.substring(0, HDFS_SCHEMA_LENGTH).equalsIgnoreCase(HDFS_SCHEMA)) {
        // if the url starts with hdfs we read from HDFS
        handleHDFS(url, config)
      } else {
        // if the URL contains :// we expect this to be a URL to a file containing scray queryspace information
        if(url.url.contains(SCHEMA_SEPARATOR)) {
          handleURL(url, config)
        } else {
          // this is either a regular file or a directory...
          handleDiskFiles(url, config)
        }
      }
    }.flatten
  }
}
