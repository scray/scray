package org.scray.example.data

import org.scalatest.WordSpec
import org.junit.Assert
import com.typesafe.scalalogging.LazyLogging
import org.scray.example.conf.JsonConfigurationParser
import org.scray.example.conf.YamlConfigurationParser

class YamlConfigParserSpecs extends WordSpec with LazyLogging {
  "YamlConfigParserSpecs" should {
    " configuration string " in {

      val configurationString = "graphiteHost:  graphite-host\n" +        
        "graphitePort: 2003\n" + 
        "kafkaBootstrapServers: kafka-broker-1:9092\n" +
        "kafkaTopic: facility\n"
        
      val configuration =  (new YamlConfigurationParser).parse(configurationString.toString())
      
      Assert.assertTrue(configuration.isDefined)
      Assert.assertEquals("graphite-host", configuration.get.graphiteHost) 
      Assert.assertEquals(2003, configuration.get.graphitePort)
      Assert.assertEquals("kafka-broker-1:9092", configuration.get.kafkaBootstrapServers)
      Assert.assertEquals("facility", configuration.get.kafkaTopic)
    }
  }
}