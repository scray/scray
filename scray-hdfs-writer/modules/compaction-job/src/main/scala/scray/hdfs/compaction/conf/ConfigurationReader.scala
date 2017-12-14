package org.scray.example.conf

import java.io.BufferedReader
import java.io.InputStreamReader

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import com.typesafe.scalalogging.LazyLogging
import scray.hdfs.compaction.conf.CompactionJobParameter

class ConfigurationReader(confFileName: String = "job-parameter.json") extends LazyLogging {

  def readConfFromHDFS: CompactionJobParameter = {
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
          line = br.readLine()
          configurationString.append(line)
        }
      } finally {
        br.close();
      }

      (new JsonConfigurationParser).parse(configurationString.toString())
    }).flatten
    
    configuration.getOrElse({
        logger.warn(s"No configuration file found. Use default configuration ${CompactionJobParameter()}")
        CompactionJobParameter()
      })

  }
}