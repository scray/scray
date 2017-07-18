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
import scray.querying.description._
import scray.querying.caching.serialization._
import scray.common.serialization.KryoPoolSerialization
import scray.common.serialization.numbers.KryoSerializerNumber
import scray.common.properties.ScrayProperties
import scray.common.properties.IntProperty
import scray.common.properties.predefined.PredefinedProperties
import scray.service.qservice.thrifscala.ScrayMetaTService
import scray.service.qservice.thrifscala.ScrayTServiceEndpoint
import scray.core.service.properties.ScrayServicePropertiesRegistration
import scray.service.qmodel.thrifscala.ScrayUUID
import com.typesafe.scalalogging.LazyLogging

trait KryoPoolRegistration {
  def registerSerializers = RegisterRowCachingSerializers()
}

abstract class ScrayStatefulTServer extends AbstractScrayTServer {
  val server = Thrift.serveIface(SCRAY_QUERY_LISTENING_ENDPOINT, ScrayStatefulTServiceImpl())
  override def getQueryServer: ListeningServer = server
  override def getVersion: String = "1.8.2"
}

abstract class ScrayStatelessTServer extends AbstractScrayTServer {
  val server = Thrift.serveIface(SCRAY_QUERY_LISTENING_ENDPOINT, ScrayStatelessTServiceImpl())
  override def getQueryServer: ListeningServer = server
  override def getVersion: String = "0.9.2"
}

abstract class AbstractScrayTServer extends KryoPoolRegistration with App with LazyLogging {
  // abstract functions to be customized
  def initializeResources: Unit
  def destroyResources: Unit
  def getQueryServer: ListeningServer
  def getVersion: String
  def configureProperties

  configureProperties

  // kryo pool registrars
  registerSerializers

  // custom init    
  initializeResources

  // the meta service is always part of the scray server
  val metaServer: ListeningServer = Thrift.serveIface(SCRAY_META_LISTENING_ENDPOINT, ScrayMetaTServiceImpl)

  // endpoint registration refresh timer
  private val refreshTimer = new JavaTimer(isDaemon = false)
  
  // refresh task handle
  private var refreshTask: Option[TimerTask] = None

  // this endpoint 
  val endpoint = ScrayTServiceEndpoint(SCRAY_QUERY_HOST_ENDPOINT.getHostString, SCRAY_QUERY_HOST_ENDPOINT.getPort)

  val refreshPeriod = EXPIRATION * 2 / 3

  def addrStr(): String =
    s"${SCRAY_QUERY_HOST_ENDPOINT.getHostString}:${SCRAY_QUERY_HOST_ENDPOINT.getPort}/${SCRAY_META_HOST_ENDPOINT.getPort}"

  // register this endpoint with all seeds and schedule regular refresh
  // the refresh loop keeps the server running
  SCRAY_SEEDS.map(inetAddr2EndpointString(_)).foreach { seedAddr =>
    val client = Thrift.newIface[ScrayMetaTService.FutureIface](seedAddr)
    if (Await.result(client.ping())) {
      logger.info(s"$addrStr adding local service endpoint ($endpoint) to $seedAddr.")
      val _ep = Await.result(client.addServiceEndpoint(endpoint))
      refreshTask = Some(refreshTimer.schedule(refreshPeriod.fromNow, refreshPeriod)(refresh(_ep.endpointId.get)))
    }
  }

  println(s"Scray Server Version ${getVersion} started on ${addrStr}. Waiting for client requests...")

  /**
   * Refresh the registry entry
   */
  def refresh(id: ScrayUUID): Unit = {
    SCRAY_SEEDS.map(inetAddr2EndpointString(_)).foreach { seedAddr =>
      try {
        val client = Thrift.newIface[ScrayMetaTService.FutureIface](seedAddr)
        if (Await.result(client.ping())) {
          logger.trace(s"$addrStr refreshing service endpoint ($id).")
          client.refreshServiceEndpoint(id)
        }
      } catch {
        case ex: Exception => logger.warn(s"Endpoint refresh failed: $ex")
      }
    }
  }

  override def finalize = { destroyResources }
}
