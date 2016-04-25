package scray.loader.osgi

import java.io.File
import java.io.FileInputStream
import java.io.IOException
import scala.util.Try
import org.apache.commons.io.IOUtils
import org.osgi.framework.BundleActivator
import org.osgi.framework.BundleContext
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.common.properties.ScrayProperties
import scray.common.properties.ScrayProperties.Phase
import scray.core.service.properties.ScrayServicePropertiesRegistrar
import scray.loader.configparser.ScrayConfigurationParser
import scray.loader.configuration.ScrayStores
import scray.loader.configuration.ScrayStores
import scray.loader.configparser.QueryspaceConfigurationFileHandler
import scala.collection.mutable.HashMap
import scray.loader.configparser.ScrayQueryspaceConfiguration
import scray.loader.ScrayLoaderQuerySpace

/**
 * Bundle activator in order to run scray service.
 * Can also be used without OSGI using FakeBundleContext
 */
class Activator extends BundleActivator with LazyLogging {
  
  val OSGI_FILENAME_PROPERTY = "scray.config.location"
  
  /**
   * starts the scray service
   */
  override def start(context: BundleContext) = {
    logger.info("Starting Scray")
    
    // start Properties registration phase
    ScrayProperties.setPhase(Phase.register)
    ScrayServicePropertiesRegistrar.register()    
    
    // read config-file property
    val filename = Option(context.getProperty(OSGI_FILENAME_PROPERTY)).getOrElse {
      val msg = s"Scray: You must provide property '$OSGI_FILENAME_PROPERTY' to load the configuration file and run Scray!"
      logger.error(msg)
      throw new NullPointerException(msg)
    }
    
    // check main config file is available
    val file = new File(filename)
    if(!file.exists() || !file.isFile()) {
      val msg = s"Scray: The main configuration file $filename is not available, but is needed to start Scray."
      logger.error(msg)
      throw new IOException(msg)
    }
    
    logger.info(s"Reading main configuration file $filename")
    val configString = Try(IOUtils.toString(new FileInputStream(file), "UTF-8")).getOrElse {
      val msg = s"Scray: The main configuration file $filename is not available, but is needed to start Scray."
      logger.error(msg)
      throw new IOException(msg)
    }
 
    // switch to config phase and load config file
    ScrayProperties.setPhase(Phase.config)
    val scrayConfiguration = ScrayConfigurationParser.parse(configString, true).get
    scrayConfiguration.service.propagate
    
    // setup connections
    Activator.scrayStores = Some(new ScrayStores(scrayConfiguration))
    
    // read configs and start queryspace registration
    QueryspaceConfigurationFileHandler.performQueryspaceUpdate(scrayConfiguration, Activator.queryspaces, Seq())
    Activator.queryspaces.map { config =>
      val qs = new ScrayLoaderQuerySpace(config._1, scrayConfiguration, config._2._2, Activator.scrayStores.get)
    }
    
    
    
    // start service
    // start update service
  }
  
  override def stop(context: BundleContext) = {
    // shutdown update service
    // shutdown service
    // unregister all queryspaces
    // shutdown connections
  }
}

object Activator {
  var scrayStores: Option[ScrayStores] = None
  val queryspaces: HashMap[String, (Long, ScrayQueryspaceConfiguration)] = new HashMap[String, (Long, ScrayQueryspaceConfiguration)]
}