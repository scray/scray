package org.scray.example.conf

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import java.io.BufferedReader
import java.io.InputStreamReader
import com.typesafe.scalalogging.LazyLogging
import scala.io.Source

class ConfigurationReader(confFile: String = "facility-state-job.yaml") extends LazyLogging {

  def readConfigruationFile: JobParameter = {
    val files = System.getenv("SPARK_YARN_CACHE_FILES")

    getConfFileString match {
      case Some(confString) => parseConfigurationString(confString)
      case None => {
        logger.warn(s"No configuration file found. Use default configuration ${JobParameter()}")
        JobParameter()
      }
    }

  }

  def parseConfigurationString(string: String): JobParameter = {

    (new YamlConfigurationParser).parse(string) match {
      case Some(conf) => conf
      case None => {
        logger.warn(s"Parse error. Use default configuration ${JobParameter()}")
        JobParameter()
      }
    }
  }

  private def getConfFileString: Option[String] = {
    if (confFile.startsWith("file://")) {
      logger.info(s"Read configuration from ${confFile}")
      Some(readConfReadFromLocalFs(confFile.split("file://")(1)))
    } else {
      if (System.getenv("SPARK_YARN_STAGING_DIR") != null) {
        val filePath = System.getenv("SPARK_YARN_STAGING_DIR") + "/" + confFile
        Some(readConfFromHDFS(filePath))
      } else {
        logger.warn("No value for SPARK_YARN_STAGING_DIR env var found. No path to read the configuration from")
        None
      }
    }
  }

  private def readConfFromHDFS(path: String): String = {

    val fs = FileSystem.get(new Configuration())
    val br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
    val configurationString = new StringBuilder

    try {
      var line = br.readLine()

      while (line != null) {
        configurationString.append(line + "\n")
        println(line)
        line = br.readLine()
      }
    } finally {
      br.close();
    }

    configurationString.toString()
  }
  
  private def readConfReadFromLocalFs(path: String): String = {
        val configurationString = new StringBuilder

    val bufferedSource = Source.fromFile(path)
    for (line <- bufferedSource.getLines) {
      configurationString.append(line + "\n")
    }

    bufferedSource.close
    
    configurationString.toString()
  }
}