package scray.loader.osgi

import com.twitter.finagle.Thrift
import com.twitter.util.{ Duration, JavaTimer, TimerTask }
import com.typesafe.scalalogging.slf4j.LazyLogging
import java.io.{ File, FileInputStream, IOException }
import org.apache.commons.io.IOUtils
import org.osgi.framework.{ BundleActivator, BundleContext }
import scala.collection.mutable.HashMap
import scala.collection.convert.decorateAsScala._
import scala.util.Try
import scray.common.properties.ScrayProperties
import scray.common.properties.ScrayProperties.Phase
import scray.core.service.properties.ScrayServicePropertiesRegistrar
import scray.loader.ScrayLoaderQuerySpace
import scray.loader.configparser.{ QueryspaceConfigurationFileHandler, ScrayConfigurationParser, ScrayQueryspaceConfiguration }
import scray.loader.configuration.ScrayStores
import scray.service.qmodel.thrifscala.ScrayUUID
import scray.service.qservice.thrifscala.{ ScrayCombinedStatefulTService, ScrayTServiceEndpoint }
import com.twitter.util.Await
import java.util.concurrent.TimeUnit
import scray.core.service.ScrayCombinedStatefulTServiceImpl
import scray.core.service.KryoPoolRegistration
import scray.loader.service.RefreshServing
import scray.core.service.SCRAY_QUERY_LISTENING_ENDPOINT

/**
 * Bundle activator in order to run scray service.
 * Can also be used without OSGI using FakeBundleContext
 */
class Activator extends KryoPoolRegistration with BundleActivator with LazyLogging {
  
  def getVersion: String = "0.9.5"
  
  val OSGI_FILENAME_PROPERTY = "scray.config.location"
  
  /**
   * starts the scray service
   */
  override def start(context: BundleContext) = {
    logger.info("Starting Scray")
    
    registerSerializers
    
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
    // *** launch combined service
    val server = Thrift.serveIface(SCRAY_QUERY_LISTENING_ENDPOINT, ScrayCombinedStatefulTServiceImpl())
    val refresher = new RefreshServing
    
    logger.info(s"Scray Combined Server (Version ${getVersion}) started on ${refresher.addrStr}. Waiting for client requests...")

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