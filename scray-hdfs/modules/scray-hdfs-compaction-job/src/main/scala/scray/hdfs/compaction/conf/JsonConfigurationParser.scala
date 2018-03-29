package org.scray.example.conf

import scala.io.Source._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import com.fasterxml.jackson.databind.DeserializationFeature
import scray.hdfs.compaction.conf.CompactionJobParameter

class JsonConfigurationParser  extends LazyLogging {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  def parse(json: String): Option[CompactionJobParameter] = {
    try {
      return Some(mapper.readValue(json, classOf[CompactionJobParameter]))
    } catch {
      case e: Throwable => {
        logger.error(s"Exception while parsing configuration ${json}. ${e.getMessage}")
        return None
      }
    }
    return None
  }
}