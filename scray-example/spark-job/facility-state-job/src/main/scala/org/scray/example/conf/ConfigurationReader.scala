package org.scray.example.conf

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import java.io.BufferedReader
import java.io.InputStreamReader
import com.typesafe.scalalogging.LazyLogging


class ConfigurationReader(confFileName: String = "facility-state-job.yaml") extends LazyLogging {

  def readConfFromHDFS: JobParameter = {
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

        while (line != null) {
          configurationString.append(line + "\n")
          line = br.readLine()
        }
      } finally {
        br.close();
      }

      (new YamlConfigurationParser).parse(configurationString.toString())
    }).flatten
    
    configuration.getOrElse({
        logger.warn(s"No configuration file found. Use default configuration ${JobParameter()}")
        JobParameter()
      })

  }
}