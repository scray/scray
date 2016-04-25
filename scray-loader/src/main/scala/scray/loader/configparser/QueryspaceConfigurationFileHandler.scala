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
  
  type UpdateCallback = (String, Option[(ScrayQueryspaceConfiguration, Long)]) => Unit
  
  val SCRAY_QUERYSPACE_CONFIG_ENDING = ".config.scray"
  
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
        callbacks.foreach { _(scannedFiles.get.name, Some((scannedFiles.get.queryspaceConfig, scannedFiles.get.version))) }
        oldqueryspaces.update(scannedFiles.get.name, (scannedFiles.get.version, scannedFiles.get.queryspaceConfig))
      } else {
        // new version? -> notify
        if(oldQs.get._1 != scannedFiles.get.version) {
          callbacks.foreach { _(scannedFiles.get.name, Some((scannedFiles.get.queryspaceConfig, scannedFiles.get.version))) }
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
  def readFileFromHDFS(filePath: Path, fs: FileSystem): String = IOUtils.toString(fs.open(filePath))
  
  
  /**
   * Scan directories and files for Queryspaces.
   * We ignore reload for now, which is controlled externally.
   */
  def scanDirectories(config: ScrayConfiguration): Seq[Try[ScannedQueryspaceConfigfiles]] = {
    config.urls.map { url =>
      if(url.url.substring(0, 7).equalsIgnoreCase("hdfs://")) {
        // if the url starts with hdfs we read from HDFS
        try {
          val path = new Path(url.url)
          val fs = FileSystem.get(new Configuration())
          if(fs.getFileStatus(path).isDir()) {
            logger.info(s"Reading directory for HDFS-URL ${url.url}, scanning for *$SCRAY_QUERYSPACE_CONFIG_ENDING...")
            // read complete directory
            val stati = fs.listStatus(path)
            stati.filter(_.getPath().getName().endsWith(SCRAY_QUERYSPACE_CONFIG_ENDING)).map { status => Try {
              val parsedFile = ScrayQueryspaceConfigurationParser.parse(readFileFromHDFS(status.getPath, fs), config, true)
              logger.info(s"Read queryspace configuration file ${status.getPath} with queryspace ${parsedFile.get.name}") 
              ScannedQueryspaceConfigfiles(status.getPath.toString(), parsedFile.get.name, parsedFile.get.version, parsedFile.get)
            }}.toSeq
          } else {
            Seq(Try {
              val parsedFile = ScrayQueryspaceConfigurationParser.parse(readFileFromHDFS(path, fs), config, true)
              logger.info(s"Read queryspace configuration file ${url.url} with queryspace ${parsedFile.get.name}") 
              ScannedQueryspaceConfigfiles(url.url, parsedFile.get.name, parsedFile.get.version, parsedFile.get)
            })
          }
        } catch {
          case e: Exception => Seq(Failure(e))
        }
      } else {
        // if the URL contains :// we expect this to be a URL to a file containing scray queryspace information
        if(url.url.contains("://")) {
          Seq(Try {
            val fileURL = new URL(url.url)
            val parsedFile = ScrayQueryspaceConfigurationParser.parse(IOUtils.toString(fileURL.openStream()), config, true)
            logger.info(s"Read queryspace configuration file ${url.url} with queryspace ${parsedFile.get.name}") 
            ScannedQueryspaceConfigfiles(url.url, parsedFile.get.name, parsedFile.get.version, parsedFile.get)
          })
        } else {
          // this is either a regular file or a directory...
          val file = new File(url.url)
          if(file.exists()) {
            if(file.isDirectory()) {
              logger.info(s"Reading directory ${url.url}, scanning for *$SCRAY_QUERYSPACE_CONFIG_ENDING...")
              val files = file.listFiles(new FilenameFilter() { 
                override def accept(dir: File, name: String): Boolean = name.endsWith(SCRAY_QUERYSPACE_CONFIG_ENDING) })
              files.map { conffile => Try {
                val parsedFile = ScrayQueryspaceConfigurationParser.parse(IOUtils.toString(new FileInputStream(conffile)), config, true)
                logger.info(s"Read queryspace configuration file ${conffile.toString()} with queryspace ${parsedFile.get.name}") 
                ScannedQueryspaceConfigfiles(conffile.toString(), parsedFile.get.name, parsedFile.get.version, parsedFile.get)
              }}.toSeq
            } else {
              Seq(Try {
                val parsedFile = ScrayQueryspaceConfigurationParser.parse(IOUtils.toString(new FileInputStream(file)), config, true)
                logger.info(s"Read queryspace configuration file ${url.url} with queryspace ${parsedFile.get.name}") 
                ScannedQueryspaceConfigfiles(url.url, parsedFile.get.name, parsedFile.get.version, parsedFile.get)
              })
            }
          } else {
            val msg = "Could not read queryspace configuration file " + url.url
            logger.warn(msg)
            Seq(Failure(new IOException(msg)))
          }
        }
      }
    }.flatten
  }
}
