package org.scray.example.cli

import scala.io.Source._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.twitter.util.Future

import java.nio.file.Paths
import akka.NotUsed
import java.util.concurrent.LinkedBlockingQueue

import scray.example.input.db.fasta.model.Facility
import com.typesafe.scalalogging.LazyLogging
import com.fasterxml.jackson.databind.DeserializationFeature

class JsonConfigurationParser  extends LazyLogging {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  def jsonReader(json: String): Option[Config] = {
    try {
      return Some(mapper.readValue(json, classOf[Config]))
    } catch {
      case e: Throwable => {
        logger.error(s"Exception while parsing configuration ${json}. ${e.getMessage}")
        return None
      }
    }
    return None
  }
}