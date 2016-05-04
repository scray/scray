package scray.loader.osgi

import com.twitter.finagle.Thrift
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.osgi.framework.{ BundleActivator, BundleContext }
import scala.collection.convert.decorateAsJava.mapAsJavaMapConverter
import scala.collection.mutable.HashMap
import scray.common.properties.{ PropertyMemoryStorage, ScrayProperties }
import scray.common.properties.ScrayProperties.Phase
import scray.core.service.{ KryoPoolRegistration, SCRAY_QUERY_LISTENING_ENDPOINT, ScrayCombinedStatefulTServiceImpl }
import scray.core.service.properties.ScrayServicePropertiesRegistrar
import scray.loader.ScrayLoaderQuerySpace
import scray.loader.configparser.{ MainConfigurationFileHandler, QueryspaceConfigurationFileHandler, ScrayQueryspaceConfiguration }
import scray.loader.configuration.ScrayStores
import scray.loader.service.RefreshServing
import scray.loader.tools.SerializationTooling
import scray.querying.Registry
import com.twitter.util.Time
import com.twitter.util.Duration
import com.twitter.util.Await
import java.util.concurrent.TimeUnit

/**
 * Bundle activator in order to run scray service.
 * Can also be used without OSGI using FakeBundleContext.
 */
class Activator extends KryoPoolRegistration with BundleActivator with LazyLogging {
  
  def getVersion: String = "0.9.5"
  
  /**
   * starts the scray service
   */
  override def start(context: BundleContext) = {
    logger.info("Starting Scray...")
    
    // register serialization for Kryo to improve performance 
    registerSerializers
    SerializationTooling.registerAdditionalKryoSerializers()
    
    // start Properties registration phase
    ScrayServicePropertiesRegistrar.register()    
    
    // read config-file property
    val filename = Option(context.getProperty(Activator.OSGI_FILENAME_PROPERTY)).getOrElse {
      val msg = s"Scray: You must provide property '${Activator.OSGI_FILENAME_PROPERTY}' to load the configuration file and run Scray!"
      logger.error(msg)
      throw new NullPointerException(msg)
    }
    
    // switch to config phase and load config file
    logger.info(s"Reading main configuration file $filename") 
    ScrayProperties.setPhase(Phase.config)
    val scrayConfiguration = MainConfigurationFileHandler.readMainConfig(filename).get
    ScrayProperties.addPropertiesStore(
        new PropertyMemoryStorage(scrayConfiguration.service.memoryMap.asJava.asInstanceOf[java.util.Map[String, Object]]))
    ScrayProperties.setPhase(Phase.use)
    
    // setup connections
    logger.info(s"Preparing store instances")
    Activator.scrayStores = Some(new ScrayStores(scrayConfiguration))
    
    // read configs and start queryspace registration
    QueryspaceConfigurationFileHandler.performQueryspaceUpdate(scrayConfiguration, Activator.queryspaces, Seq())
    Activator.queryspaces.map { config =>
      logger.info(s"Registering queryspace ${config._1}")
      val qs = new ScrayLoaderQuerySpace(config._1, scrayConfiguration, config._2._2, Activator.scrayStores.get)
      Registry.registerQuerySpace(qs, Some(0))
    }
    
    // start service
    // *** launch combined service, by accessing lazy var...
    server
    // register shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(
      new Runnable {
        def run() {
          shutdownServer()
        }
      }
    ))

    // start fail-over refresh daemon thread
    Activator.refresher = Some(new RefreshServing)
    
    logger.info(s"Scray Combined Server (Version ${getVersion}) started on ${Activator.refresher.get.addrStr}. Waiting for client requests...")

    // start update service
  }

  lazy val server = {
    val srv = Thrift.serveIface(SCRAY_QUERY_LISTENING_ENDPOINT, ScrayCombinedStatefulTServiceImpl())
    logger.info(s"Going to install listening combined Scray meta- and query-service on ${srv.boundAddress}...")
    srv
  }
  
  override def stop(context: BundleContext) = {
    // shutdown update service
    Activator.refresher.map { refresher =>
      refresher.destroyResources
      refresher.client.map(_.close())
    }
    
    // shutdown service
    shutdownServer()
    
    // unregister all queryspaces
    Registry.getQuerySpaceNames().foreach { name =>
      Registry.updateQuerySpace(name, Set())
    }
    
    // shutdown connections
    
  }
  
  private def shutdownServer() = {
    Await.ready(server.close(Duration(15, TimeUnit.SECONDS)))
  }
}

object Activator {
  val OSGI_FILENAME_PROPERTY = "scray.config.location"
  
  var scrayStores: Option[ScrayStores] = None
  val queryspaces: HashMap[String, (Long, ScrayQueryspaceConfiguration)] = new HashMap[String, (Long, ScrayQueryspaceConfiguration)]
  var refresher: Option[RefreshServing] = None 
}