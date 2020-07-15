package org.scray.examples.ingest.prometheus

import java.util.ArrayList
import scala.collection.JavaConverters._
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.LazyLogging

class Client extends LazyLogging {

  def getMetricsList(url: String): List[String] = {
    val metricsList = new ArrayList[String];

    val client = HttpClients.createDefault();

    val method = new HttpGet(url);
    // Execute the method.
    val response = client.execute(method);

    val entity = response.getEntity();
    if (entity != null) {

      val instream = entity.getContent();

      val metricsRawData = scala.io.Source.fromInputStream(instream).mkString
      IOUtils.closeQuietly(instream);

      val iter = this.parseString(metricsRawData).get("data").elements
      while (iter.hasNext()) {
        metricsList.add(iter.next.asText())
      }
    }

    response.close();
    metricsList.asScala.toList
  }

  def getMetricsData(metrics: String): List[String] = {

    val objectMapper = new ObjectMapper();
    val parsedJsonString = this.parseString(metrics)

    if (parsedJsonString.get("status").asText().equals("error")) {
      logger.error(s"Error while fetching data from prometheus api ${parsedJsonString.toString}")
      List.empty[String]
    } else {
      parsedJsonString
        .get("data")
        .get("result")
        .elements().asScala.map(node => { node.toString() }).toList
    }

  }

  def getMetricValues(url: String): Option[String] = {
    var metricsRawData: Option[String] = None

    val client = HttpClients.createDefault();

    val method = new HttpGet(url);
    // Execute the method.
    val response = client.execute(method);

    val entity = response.getEntity();
    if (entity != null) {

      val instream = entity.getContent();

      metricsRawData = Some(scala.io.Source.fromInputStream(instream).mkString)
      IOUtils.closeQuietly(instream);

    }

    response.close();

    metricsRawData
  }

  def getExtractedMetricValues(url: String): List[String] = {
    var metricsRawData: List[String] = List[String]()

    val client = HttpClients.createDefault();

    val method = new HttpGet(url);
    // Execute the method.
    val response = client.execute(method);

    val entity = response.getEntity();
    if (entity != null) {

      val instream = entity.getContent();

      metricsRawData = getMetricsData(scala.io.Source.fromInputStream(instream).mkString)
      IOUtils.closeQuietly(instream);

    }

    response.close();

    metricsRawData
  }

  private def parseString(jsonString: String): JsonNode = {
    val jsonFactory = new ObjectMapper()
    jsonFactory.readTree(jsonString)
  }
}