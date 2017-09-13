package scray.querying.sync.conf

import org.yaml.snakeyaml.Yaml
import java.beans.IntrospectionException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.base.Joiner;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.introspector.MissingProperty;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.twitter.chill.config.Config

/**
 * Load scray sync configuration
 */
object SyncConfigurationLoader extends LazyLogging {

  val DEFAULT_CONFIGURATION = "scray-sync.yaml";

  private def getConfigURL: Option[URL] = {
    var configUrl = System.getProperty("cassandra.config");

    if (configUrl == null) {
      logger.debug(s"Option -Dcassandra.config not foud. Try to use ${DEFAULT_CONFIGURATION}")
      configUrl = DEFAULT_CONFIGURATION;
    }

    var url: Option[URL] = None
    try {
      url = Some(new URL(configUrl))
      url.get.openStream().close();
    } catch {
      case e: Exception => {
        val loader = SyncConfigurationLoader.getClass.getClassLoader
        val resourceURL = loader.getResource(configUrl)

        if (resourceURL == null) {
          logger.warn(s"No valid configuration file found in ${configUrl}. Use default values")
          url = None
        } else {
          url = Some(resourceURL)
        }
      }
    }

    url
  }

  def loadConfig: SyncConfiguration = this.synchronized {
    var conf: Option[SyncConfiguration] = None

    getConfigURL.map(url => {

      if (conf.isDefined) {
        logger.debug(s"Configuration already loaded. Use existing configuration: ${conf.get}")
      } else {

        conf = Some(new SyncConfiguration)
        conf.map(confe => {
          try {
            val yaml = new Yaml;
            val yamlData = yaml.load(url.openStream).asInstanceOf[Map[String, String]]

            if (yamlData.get("dbSystem") != null) {
              confe.dbSystem = yamlData.get("dbSystem")
            }

            if (yamlData.get("tableName") != null) {
              confe.tableName = yamlData.get("tableName")
            }

            if (yamlData.get("replication") != null) {
              confe.replicationSetting = yamlData.get("replication")
            }
          } catch {
            case e: YAMLException => {
              logger.error(s"Invalid sync configuration yaml: ${url}, ${e.getMessage}")
              conf = None
            }
          }
        })
      }
    })
    
    // Use default configuration if no configuration file was provided
    if(conf.isDefined) {
      conf.get
    } else {
      new SyncConfiguration      
    }
  }
}