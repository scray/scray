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
class SyncConfigurationLoader(path: String = "scray-sync.yaml") extends LazyLogging {

  def loadConfig: SyncConfiguration = {

    val url: URL = (new File(path)).toURI().toURL

    val yaml = new Yaml;
    val yamlData = yaml.load(url.openStream).asInstanceOf[Map[String, String]]
    
    val conf =  new SyncConfiguration

    
    if(yamlData.get("dbSystem") != null) {
      conf.dbSystem = yamlData.get("dbSystem")
    }
    
    if(yamlData.get("tableName") != null) {
      conf.tableName = yamlData.get("tableName")
    }
    
    if(yamlData.get("replication") != null) {
      conf.replicationSetting = yamlData.get("replication")
    }
    
    conf
  }
}