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
package scray.loader.service

import com.twitter.finagle.Thrift
import com.twitter.util.{ Await, Duration, JavaTimer, TimerTask }
import com.typesafe.scalalogging.slf4j.LazyLogging
import java.util.concurrent.TimeUnit
import scala.collection.convert.decorateAsScala.asScalaSetConverter
import scray.core.service.{ EXPIRATION, SCRAY_QUERY_HOST_ENDPOINT, SCRAY_SEEDS, inetAddr2EndpointString }
import scray.service.qmodel.thrifscala.ScrayUUID
import scray.service.qservice.thrifscala.{ ScrayCombinedStatefulTService, ScrayTServiceEndpoint }


/**
 * handles refreshment of ip addresses for the scray service
 */
// scalastyle:off no.finalize
class RefreshServing extends LazyLogging {
  
  var shownSeedException = false

  /**
   * the client which talks to the seed nodes to ping
   */
  var client: Option[ScrayCombinedStatefulTService.FutureIface] = None

  /**
   *  refresh task handle
   */
  private var refreshTask: Option[TimerTask] = None

  /**
   * this endpoint, as provided in the config file 
   */
  lazy val endpoint = ScrayTServiceEndpoint(SCRAY_QUERY_HOST_ENDPOINT.getHostString, SCRAY_QUERY_HOST_ENDPOINT.getPort)

  val refreshPeriod = EXPIRATION * 2 / 3

  /**
   *  endpoint registration refresh timer
   */
  private val refreshTimer = new JavaTimer(isDaemon = false) {
    override def logError(t: Throwable) {
      logger.error("Could not refresh.", t)
    }
  }
  
  // register this endpoint with all seeds and schedule regular refresh
  // the refresh loop keeps the server running
  SCRAY_SEEDS.asScala.map(inetAddr2EndpointString(_)).foreach { seedAddr =>
    try {
      if (Await.result(getClient(seedAddr).ping(), RefreshServing.RESULT_WAITING_TIME)) {
        logger.debug(s"$addrStr adding local service endpoint ($endpoint) to $seedAddr.")
        val _ep = Await.result(getClient(seedAddr).addServiceEndpoint(endpoint), RefreshServing.RESULT_WAITING_TIME)
        // refreshTask = Some(refreshTimer.schedule(refreshPeriod.fromNow, refreshPeriod)(refresh(_ep.endpointId.get)))
        refreshTask = Some(refreshTimer.schedule(refreshPeriod.fromNow, refreshPeriod)(refresh(_ep.endpointId.get)))
      }
    } catch {
      case ex: Exception => {
        shownSeedException match {
          case true => shownSeedException
          case false => 
            logger.error(s"Error while intializing refresh service", ex)
            shownSeedException = true 
        }
      }
    }
  }

  def addrStr(): String =
    s"${SCRAY_QUERY_HOST_ENDPOINT.getHostString}:${SCRAY_QUERY_HOST_ENDPOINT.getPort}"

  /**
   * return or create a thrift client through Finagle
   */
  private def getClient(seedAddr: String): ScrayCombinedStatefulTService.FutureIface = {
    client.getOrElse {
      logger.info("Initializing thrift-client ")
      val clientIface = Thrift.newIface[ScrayCombinedStatefulTService.FutureIface](seedAddr)
      client = Some(clientIface)
      clientIface
    }
  }

  /**
   * Refresh the registry entry
   */
  def refresh(id: ScrayUUID, time: Int = 1): Unit = {
    SCRAY_SEEDS.asScala.map(inetAddr2EndpointString(_)).foreach { seedAddr =>
      try {
        logger.trace(s"$addrStr trying to refresh service endpoint ($id).")
        if (Await.result(getClient(seedAddr).ping(), RefreshServing.RESULT_WAITING_TIME)) {
          logger.debug(s"$addrStr refreshing service endpoint ($id).")
          // client.refreshServiceEndpoint(id)
          Await.result(getClient(seedAddr).addServiceEndpoint(endpoint), RefreshServing.RESULT_WAITING_TIME)
        }
      } catch {
        case ex: Exception =>
          client.map { _.shutdown(None) }
          client = None
          getClient(seedAddr)
          if (time < 4) {
            logger.warn(s"Endpoint refresh failed, time $time: $ex", ex)
            // scalastyle:off magic.number
            Thread.sleep(10000)
            // scalastyle:on magic.number
            refresh(id, time + 1)
          } else {
            logger.warn("Endpoint refresh failed. Retry maximum exceeded. Exiting.")
          }
      }
    }
  }

  /**
   * might (!) be called on shutdown
   */
  def destroyResources: Unit = {}

  override def finalize: Unit = {
    client = None
    destroyResources
  }
}
// scalastyle:on no.finalize

object RefreshServing {
  // scalastyle:off magic.number
  val RESULT_WAITING_TIME = Duration(20, TimeUnit.SECONDS)
  // scalastyle:on magic.number
}
