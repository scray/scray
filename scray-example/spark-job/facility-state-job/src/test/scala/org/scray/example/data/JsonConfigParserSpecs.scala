package org.scray.example.data

import org.scalatest.WordSpec
import org.junit.Assert
import com.typesafe.scalalogging.LazyLogging
import org.scray.example.conf.JsonConfigurationParser

class JsonConfigParserSpecs extends WordSpec with LazyLogging {
  
  " JsonConfigParser" should {
    " pars configuration json string " in {
      val parser = new  JsonConfigurationParser
      
      Assert.assertTrue(parser.parse("""{"master": "yarn-cluster",             "batch": false,  "graphiteHost":  "127.0.0.1",          "graphitePort": 2003,    "kafkaBootstrapServers": "10.11.22.34:9092",  "kafkaTopic": "facility",    "windowDuration": "20 seconds",  "slideDuration":  "20 seconds",  "watermark": "0 milliseconds",    "kafkaDataSchemaAsJsonExample":  "{\"type\":\"struct\",\"fields\":[{\"name\":\"type\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"state\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"timestamp\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}"}""").isDefined)
    }
  }
  
}