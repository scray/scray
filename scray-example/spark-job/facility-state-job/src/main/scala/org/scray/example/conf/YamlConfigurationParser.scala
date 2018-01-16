package org.scray.example.conf

import org.yaml.snakeyaml.Yaml
import com.typesafe.scalalogging.LazyLogging
import org.yaml.snakeyaml.error.YAMLException
import java.util.LinkedHashMap

class YamlConfigurationParser extends LazyLogging {

  def parse(txt: String): Option[JobParameter] = {

    val yaml = new Yaml;
    val yamlData = yaml.load(txt).asInstanceOf[LinkedHashMap[String, String]]
    val confObject = new JobParameter

    try {

      if(yamlData.get("graphiteHost") != null ) {
        confObject.graphiteHost = yamlData.get("graphiteHost")
      } else {
        logger.debug(s"GraphiteHost not defined use default: ${confObject.graphiteHost}")
      }


      if(yamlData.get("graphitePort") != null) {
        confObject.graphitePort = yamlData.get("graphitePort").asInstanceOf[Integer]
      } else {
        logger.debug(s"GraphitePort not defined use default: ${confObject.graphitePort}")
      }

      if(yamlData.get("kafkaBootstrapServers") != null) {
        confObject.kafkaBootstrapServers = yamlData.get("kafkaBootstrapServers")
      } else {
        logger.debug(s"kafkaBootstrapServers not defined use default: ${confObject.kafkaBootstrapServers}")
      }

      if(yamlData.get("kafkaTopic") != null) {
        confObject.kafkaTopic = yamlData.get("kafkaTopic")
      } else {
        logger.debug(s"kafkaTopic not defined use default: ${confObject.kafkaTopic}")
      }
            
      if(yamlData.get("graphiteRetries") != null) {
        confObject.graphiteRetries = yamlData.get("graphiteRetries").asInstanceOf[Integer]
      } else {
        logger.debug(s"graphiteRetries not defined use default: ${confObject.graphiteRetries}")
      }
      
      if(yamlData.get("sparkMaster") != null) {
        confObject.sparkMaster = yamlData.get("sparkMaster")
      } else {
        logger.debug(s"sparkMaster not defined use default: ${confObject.sparkMaster}")
      }
      
      if(yamlData.get("checkpointPath") != null) {
        confObject.checkpointPath = yamlData.get("checkpointPath")
      } else {
        logger.debug(s"checkpointPath not defined use default: ${confObject.checkpointPath}")
      }
      
      if(yamlData.get("windowDuration") != null) {
        confObject.windowDuration = yamlData.get("windowDuration")
      } else {
        logger.debug(s"windowDuration not defined use default: ${confObject.windowDuration}")
      }
      
      if(yamlData.get("slideDuration") != null) {
        confObject.slideDuration = yamlData.get("slideDuration")
      } else {
        logger.debug(s"slideDuration not defined use default: ${confObject.slideDuration}")
      }
      
      if(yamlData.get("watermark") != null) {
        confObject.watermark = yamlData.get("watermark")
      } else {
        logger.debug(s"slideDuration not defined use default: ${confObject.watermark}")
      }
      
      if(yamlData.get("batchFilePath") != null) {
        confObject.batchFilePath = yamlData.get("batchFilePath")
      } else {
        logger.debug(s"slideDuration not defined use default: ${confObject.batchFilePath}")
      }
      
      if(yamlData.get("batchDataSource") != null) {
        
        if(yamlData.get("batchDataSource").equals("TEXT")) {
          confObject.batchDataSource = TEXT
        } else if (yamlData.get("batchDataSource").equals("CASSANDRA")) {
          confObject.batchDataSource = CASSANDRA
        } else {
          logger.debug(s"batchDataSource not defined use default: ${confObject.batchDataSource}. Possible values are TEXT ore CASSANDRA")
        }
      }
    } catch {
      case e: YAMLException => {
        logger.error(s"Invalid job parameter yaml:  ${e.getMessage}")
         Some(new JobParameter)
      }
    }

    Some(confObject) 
  }
}