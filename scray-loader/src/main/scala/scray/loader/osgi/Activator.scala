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
import com.typesafe.scalalogging.slf4j.LazyLogging

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

/**
 * Bundle activator in order to run scray thrift service.
 * Can also be used without OSGI using FakeBundleContext.
 */
class Activator extends KryoPoolRegistration with BundleActivator with LazyLogging {
  
  /**
   * must be increased with each new version
   */
  def getVersion: String = "0.9.5"
  
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
   * registers all query spaces
   */
  private def registerQuerySpaces(scrayConfiguration: ScrayConfiguration) = {
    QueryspaceConfigurationFileHandler.performQueryspaceUpdate(scrayConfiguration, Activator.queryspaces, Seq())
    Activator.queryspaces.map { config =>
      logger.info(s"Registering queryspace ${config._1}")
      val qs = new ScrayLoaderQuerySpace(config._1, scrayConfiguration, config._2._2, Activator.scrayStores.get)
      Registry.registerQuerySpace(qs, Some(0))
    }    
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
    new Thread("Scray Service Manager") {
      override def run(): Unit = {
        logger.info("Starting Scray...")
        
        registerAllSerializers
        
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
        // *** launch combined service, by accessing lazy var...
        server
        // register shutdown hook
        registerShutdownHook
        
        // start fail-over refresh daemon thread
        Activator.refresher = Some(new RefreshServing)
        
        logger.info(s"Scray Combined Server (Version ${getVersion}) started on ${Activator.refresher.get.addrStr}. Waiting for client requests...")
    
        // start update service
        while(Activator.keepRunning) {
          // scalastyle:off magic.number
          Thread.sleep(5000)
          // scalastyle:on magic.number
        }
        
        initializeShutdown(context)
      }
    }.start()
  }

  lazy val server = {
    val srv = Thrift.serveIface(SCRAY_QUERY_LISTENING_ENDPOINT, ScrayCombinedStatefulTServiceImpl())
    logger.info(s"Going to install listening combined Scray meta- and query-service on ${srv.boundAddress}...")
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
    // scalastyle:off magic.number
    Await.ready(server.close(Duration(15, TimeUnit.SECONDS)))
    // scalastyle:on magic.number
  }
}

object Activator {
  val OSGI_FILENAME_PROPERTY = "scray.config.location"
  
  var scrayStores: Option[ScrayStores] = None
  val queryspaces: HashMap[String, (Long, ScrayQueryspaceConfiguration)] = new HashMap[String, (Long, ScrayQueryspaceConfiguration)]
  var refresher: Option[RefreshServing] = None
  var keepRunning: Boolean = true
}
