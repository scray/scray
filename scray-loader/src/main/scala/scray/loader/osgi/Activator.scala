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
package scray.loader.osgi

import com.twitter.finagle.Thrift
import com.twitter.util.{ Await, Duration }
import com.typesafe.scalalogging.LazyLogging

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import org.osgi.framework.{ BundleActivator, BundleContext }

import scala.collection.convert.decorateAsJava.mapAsJavaMapConverter
import scala.collection.mutable.HashMap

import scray.common.properties.{ PropertyMemoryStorage, ScrayProperties }
import scray.common.properties.ScrayProperties.Phase
import scray.core.service.{ KryoPoolRegistration, SCRAY_QUERY_LISTENING_ENDPOINT, ScrayCombinedStatefulTServiceImpl }
import scray.core.service.properties.ScrayServicePropertiesRegistrar
import scray.loader.ScrayLoaderQuerySpace
import scray.loader.configparser.{ MainConfigurationFileHandler, QueryspaceConfigurationFileHandler, ScrayConfiguration, ScrayQueryspaceConfiguration }
import scray.loader.configuration.ScrayStores
import scray.loader.service.RefreshServing
import scray.loader.tools.SerializationTooling
import scray.querying.Registry
import scray.loader.VERSION
import scray.core.service.ScrayStatelessTServiceImpl
import com.twitter.util.FuturePool
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.LinkedBlockingQueue
import com.twitter.util.ExecutorServiceFuturePool
import scray.service.qservice.thriftjava.ScrayStatefulTService
import scray.core.service.spools.TSpoolRack


/**
 * Bundle activator in order to run scray thrift service.
 * Can also be used without OSGI using FakeBundleContext.
 */
class Activator extends KryoPoolRegistration with BundleActivator with LazyLogging {
  
  /**
   * must be increased with each new version
   */
  def getVersion: String = VERSION

  
  // count-down-latch for synchronizing the shutdown process
  val finalizationLatch = new CountDownLatch(1)
  
  // whether this is a stateless service
  private var statelessService = false

  private var futurePool: Option[FuturePool] = None 
  
  /**
   * read main config filename from BundleContext or throw 
   */
  private def getFilename(context: BundleContext): String = Option(context.getProperty(Activator.OSGI_FILENAME_PROPERTY)).
    getOrElse {
      val msg = s"""Scray: You must provide property '${Activator.OSGI_FILENAME_PROPERTY}' to 
      load the configuration file and run Scray!"""
      logger.error(msg)
      throw new NullPointerException(msg)
    }
  
  /**
   * whether the service should be installed stateless
   */
  private def getStateless(context: BundleContext): Boolean = Option(context.getProperty(Activator.OSGI_SCRAY_STATELESS)).
    map(str => if(str.trim().toUpperCase() == "TRUE") true else false).getOrElse(false)
  
  /**
   * registers all query spaces
   */
  private def registerQuerySpaces(scrayConfiguration: ScrayConfiguration) = {
    QueryspaceConfigurationFileHandler.performQueryspaceUpdate(scrayConfiguration, Activator.queryspaces, Seq())
    Activator.queryspaces.map { config =>
      logger.debug(s"Registering queryspace ${config._1}")
      val qs = new ScrayLoaderQuerySpace(config._1, scrayConfiguration, config._2._2, Activator.scrayStores.get, futurePool.get)
      Registry.registerQuerySpace(qs, Some(0))
    }    
  }
  
  def getPoolSize(context: BundleContext) = {
    Option(context.getProperty(Activator.POOLSIZE_PROPERTY)).map(str => str.toInt).getOrElse(scray.loader.DEFAULT_THREAD_POOL_SIZE)
  }
  
  private def createFuturePool(context: BundleContext) = {
    val poolSize = getPoolSize(context)
    FuturePool(new ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.SECONDS, 
        new LinkedBlockingQueue[Runnable](10 * poolSize), new ThreadPoolExecutor.CallerRunsPolicy()))
  }
    
  
  /**
   * registers shutdown hook for emergency shutdowns
   */
  private def registerShutdownHook =
    Runtime.getRuntime().addShutdownHook(new Thread(
      new Runnable {
        override def run(): Unit = {
          shutdownServer()
        }
      }
    ))

  /**
   * register all serializers and switch phase
   */
  private def registerAllSerializers = {
    // register serialization for Kryo to improve performance 
    registerSerializers
    SerializationTooling.registerAdditionalKryoSerializers()
    
    // start Properties registration phase
    ScrayServicePropertiesRegistrar.register()    
  }
  
  private def loadMainConfigFile(filename: String): ScrayConfiguration = {
    ScrayProperties.setPhase(Phase.config)
    val scrayConfiguration = MainConfigurationFileHandler.readMainConfig(filename).get
    ScrayProperties.addPropertiesStore(
        new PropertyMemoryStorage(scrayConfiguration.service.memoryMap.asJava.asInstanceOf[java.util.Map[String, Object]]))
    ScrayProperties.setPhase(Phase.use)
    scrayConfiguration
  }
  
  
  /**
   * starts the scray service
   */
  override def start(context: BundleContext): Unit = {
    val latch = new CountDownLatch(1)
    new Thread("Scray Service Manager") {
      override def run(): Unit = {
        logger.info("Starting Scray...")
        
        registerAllSerializers
        
        futurePool = Some(createFuturePool(context))
        futurePool.get match {
          case esfp: ExecutorServiceFuturePool =>
            logger.info(s"Set-up pool with ${getPoolSize(context)} threads for general use")
          case _ => 
            logger.info(s"Set-up thread-pool for general use")
        }
        
        
        // read config-file property
        val filename = getFilename(context)
        
        // switch to config phase and load config file
        logger.info(s"Reading main configuration file $filename")
        val scrayConfiguration = loadMainConfigFile(filename)
        
        // setup connections
        logger.info(s"Preparing store instances")
        Activator.scrayStores = Some(new ScrayStores(scrayConfiguration))
        
        // read configs and start queryspace registration
        registerQuerySpaces(scrayConfiguration)
        
        // start service
        statelessService = getStateless(context)
        // *** launch combined service, by accessing lazy var...
        server
        // register shutdown hook
        registerShutdownHook
        
        // start fail-over refresh daemon thread
        Activator.refresher = Some(new RefreshServing)
        
        logger.info(s"Scray Combined Server (Version ${getVersion}) started on ${Activator.refresher.get.addrStr}. Waiting for client requests...")
    
        latch.countDown()
        // start update service
        while(Activator.keepRunning) {
          logger.debug("Scray still keeps running for next 5s...")
          // scalastyle:off magic.number
          Thread.sleep(5000)
          // scalastyle:on magic.number
        }
        
        finalizationLatch.countDown()
        initializeShutdown(context)
      }
    }.start()
    latch.await(2, TimeUnit.MINUTES)
  }

  lazy val server = {
    val srv = if(statelessService) {
      Thrift.server.serveIface(SCRAY_QUERY_LISTENING_ENDPOINT, ScrayStatelessTServiceImpl())
    } else {

      Thrift.server.serveIface(SCRAY_QUERY_LISTENING_ENDPOINT, new ScrayCombinedStatefulTServiceImpl(TSpoolRack))
    }
    logger.info(s"Going to install listening Scray meta- and query-service on ${srv.boundAddress}...")
    srv
  }
  
  def initializeShutdown(context: BundleContext): Unit = {
    stop(context)
  }
  
  override def stop(context: BundleContext): Unit = {
    
    // shutdown update service
    Activator.refresher.map { refresher =>
      refresher.destroyResources
      refresher.client = None
    }
    
    // shutdown service
    shutdownServer()
    
    // unregister all queryspaces
    Registry.getQuerySpaceNames().foreach { name =>
      Registry.updateQuerySpace(name, Set())
    }
    
    // shutdown connections
    
  }
  
  private def shutdownServer(): Unit = {
    Activator.keepRunning = false
    futurePool.get match {
      case esfp: ExecutorServiceFuturePool =>
        logger.info(s"Shutting down generic thread-pool")
        esfp.executor.shutdown()
        if(!esfp.executor.awaitTermination(5, TimeUnit.SECONDS)) {
          logger.info(s"Shutdown failed. Forcing shutdown of generic thread-pool")
          esfp.executor.shutdownNow()
        }
      case _ => 
        logger.warn(s"Could not shutdown worker pool. Leaving it running.")
    }
    
    finalizationLatch.await(10, TimeUnit.SECONDS)
    // scalastyle:off magic.number
    Await.ready(server.close(Duration(15, TimeUnit.SECONDS)))
    // scalastyle:on magic.number
    
  }
}

object Activator {
  val OSGI_FILENAME_PROPERTY = "scray.config.location"
  val OSGI_SCRAY_STATELESS = "scray.config.stateless"
  val POOLSIZE_PROPERTY = "scray.config.poolsize"
  
  var scrayStores: Option[ScrayStores] = None
  val queryspaces: HashMap[String, (Long, ScrayQueryspaceConfiguration)] = new HashMap[String, (Long, ScrayQueryspaceConfiguration)]
  var refresher: Option[RefreshServing] = None
  var keepRunning: Boolean = true
}
