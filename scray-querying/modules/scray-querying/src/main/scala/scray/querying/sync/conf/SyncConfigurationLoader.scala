// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.introspector.MissingProperty;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;
import com.twitter.chill.config.Config
import com.typesafe.scalalogging.LazyLogging

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