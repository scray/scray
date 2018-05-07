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

package scray.core.service

import java.net.InetSocketAddress
import com.esotericsoftware.kryo.Kryo
import scala.collection.JavaConversions._
import com.twitter.finagle.ListeningServer
import com.twitter.finagle.Thrift
import com.twitter.util.Await
import com.twitter.util.Duration
import com.twitter.util.Time
import com.twitter.util.TimerTask
import com.twitter.util.JavaTimer
import java.util.concurrent.TimeUnit
import scray.querying.description._
import scray.querying.caching.serialization._
import scray.common.serialization.KryoPoolSerialization
import scray.common.serialization.numbers.KryoSerializerNumber
import scray.common.properties.ScrayProperties
import scray.common.properties.IntProperty
import scray.common.properties.predefined.PredefinedProperties
import scray.service.qservice.thrifscala.ScrayCombinedStatefulTService
import scray.service.qservice.thrifscala.ScrayTServiceEndpoint
import scray.core.service.properties.ScrayServicePropertiesRegistration
import scray.service.qmodel.thrifscala.ScrayUUID
import com.typesafe.scalalogging.LazyLogging
import org.apache.thrift.protocol.TBinaryProtocol
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http._
import org.apache.thrift.transport.TSocket

abstract class ScrayCombinedStatefulTServer extends KryoPoolRegistration with App with LazyLogging {
  // abstract functions to be customized
  def initializeResources: Unit
  def destroyResources: Unit
  def configureProperties

  // read properties
  configureProperties

  // kryo pool registrars
  registerSerializers

  // custom init    
  initializeResources

  // launch combined service
  val server = Thrift.server.serveIface(SCRAY_QUERY_LISTENING_ENDPOINT, ScrayCombinedStatefulTServiceImpl())
  def getVersion: String = "0.9.2"

  // endpoint registration refresh timer
  private val refreshTimer = new JavaTimer(isDaemon = false) {
    override def logError(t: Throwable) {
      logger.error("Could not refresh.", t)
    }
  }

  var client: Option[ScrayCombinedStatefulTService.FutureIface] = None
  
  // refresh task handle
  private var refreshTask: Option[TimerTask] = None

  // this endpoint 
  val endpoint = ScrayTServiceEndpoint(SCRAY_QUERY_HOST_ENDPOINT.getHostString, SCRAY_QUERY_HOST_ENDPOINT.getPort)

  val refreshPeriod = EXPIRATION * 2 / 3

  def addrStr(): String =
    s"${SCRAY_QUERY_HOST_ENDPOINT.getHostString}:${SCRAY_QUERY_HOST_ENDPOINT.getPort}"

  // register this endpoint with all seeds and schedule regular refresh
  // the refresh loop keeps the server running
  SCRAY_SEEDS.map(inetAddr2EndpointString(_)).foreach { seedAddr =>
    try {
      if (Await.result(getClient(seedAddr).ping(), Duration(20, TimeUnit.SECONDS))) {
        logger.debug(s"$addrStr adding local service endpoint ($endpoint) to $seedAddr.")
        val _ep = Await.result(getClient(seedAddr).addServiceEndpoint(endpoint), Duration(20, TimeUnit.SECONDS))
        // refreshTask = Some(refreshTimer.schedule(refreshPeriod.fromNow, refreshPeriod)(refresh(_ep.endpointId.get)))
        refreshTask = Some(refreshTimer.schedule(refreshPeriod.fromNow, refreshPeriod)(refresh(_ep.endpointId.get)))
      }
    } catch {
      case ex: Exception => {
        
      }
    }
  }

  println(s"Scray Combined Server (Version ${getVersion}) started on ${addrStr}. Waiting for client requests...")

  private def getClient(seedAddr: String): ScrayCombinedStatefulTService.FutureIface = {
    client.getOrElse {
      logger.info("Initializing thrift-client ")
      val clientIface = Thrift.client.newIface[ScrayCombinedStatefulTService.FutureIface](seedAddr)
      client = Some(clientIface)
      clientIface
    }
  }
  
  /**
   * Refresh the registry entry
   */
  def refresh(id: ScrayUUID, time: Int = 1): Unit = {
    SCRAY_SEEDS.map(inetAddr2EndpointString(_)).foreach { seedAddr =>
      try {
        logger.trace(s"$addrStr trying to refresh service endpoint ($id).")
        if (Await.result(getClient(seedAddr).ping(), Duration(20, TimeUnit.SECONDS))) {
          logger.debug(s"$addrStr refreshing service endpoint ($id).")
          // client.refreshServiceEndpoint(id)
          Await.result(getClient(seedAddr).addServiceEndpoint(endpoint), Duration(20, TimeUnit.SECONDS))
        }
      } catch {
        case ex: Exception => 
          client = None
          getClient(seedAddr)
          if(time < 4) {
            logger.warn(s"Endpoint refresh failed, time $time: $ex", ex)
            Thread.sleep(10000)
            refresh(id, time + 1)
          } else {
            logger.warn("Endpoint refresh failed. Retry maximum exceeded. Exiting.")
          }
      }
    }
  }

  override def finalize = { 
    client = None
    destroyResources
  }
}
