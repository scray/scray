package org.scray.example.cli

import org.apache.spark.SparkFiles
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import java.io.BufferedReader
import java.io.InputStreamReader
import com.typesafe.scalalogging.LazyLogging

class ConfigurationReader(confFileName: String = "facility-state-job.conf") extends LazyLogging {

  def readConfFromHDFS: Config = {
    val files = System.getenv("SPARK_YARN_CACHE_FILES")
    
    def getConfFilePath: Option[String] = {
      if (System.getenv("SPARK_YARN_STAGING_DIR") != null) {
        val filePath = System.getenv("SPARK_YARN_STAGING_DIR") + "/" + confFileName
        Some(filePath)
      } else {
        logger.warn("No value for SPARK_YARN_STAGING_DIR env var found. No path to read the configuration from")
        None
      }
    }

    val configuration = getConfFilePath.map(path => {

      logger.debug(s"Read job configuration from ${path}")

      val fs = FileSystem.get(new Configuration())
      val br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
      val configurationString = new StringBuilder

      try {
        var line = br.readLine()
        configurationString.append(line)

        while (line != null) {
          configurationString.append(br.readLine())
        }
      } finally {
        br.close();
      }

      (new JsonConfigurationParser).parse(configurationString.toString())
    }).flatten
    
    configuration.getOrElse({
        logger.warn(s"No configuration file found. Use default configuration ${Config()}")
        Config()
      })

  }
}