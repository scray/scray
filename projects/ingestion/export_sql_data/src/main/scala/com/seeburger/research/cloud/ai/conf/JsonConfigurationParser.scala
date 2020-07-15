package com.seeburger.research.cloud.ai.conf

import scala.io.Source._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.DeserializationFeature
import com.typesafe.scalalogging.LazyLogging

class JsonConfigurationParser  extends LazyLogging {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  def parse(json: String): Option[JobParameter] = {
    try {
      return Some(mapper.readValue(json, classOf[JobParameter]))
    } catch {
      case e: Throwable => {
        logger.error(s"Exception while parsing configuration ${json}. ${e.getMessage}")
        return None
      }
    }
    return None
  }
}