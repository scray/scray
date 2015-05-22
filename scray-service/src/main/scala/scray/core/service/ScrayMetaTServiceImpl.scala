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

import java.util.UUID
import com.twitter.util.Future
import com.twitter.util.Time
import scray.service.qmodel.thrifscala.ScrayUUID
import scray.service.qservice.thrifscala.ScrayTServiceEndpoint
import scray.service.qservice.thrifscala.ScrayMetaTService
import org.slf4j.LoggerFactory
import scray.common.properties.ScrayProperties
import scray.core.service.properties.ScrayServicePropertiesRegistrar
import com.twitter.util.Duration
import com.twitter.util.Time
import com.twitter.util.JavaTimer

object ScrayMetaTServiceImpl extends ScrayMetaTService[Future] {

  private val logger = LoggerFactory.getLogger(ScrayMetaTServiceImpl.getClass)

  private val endpoints = scala.collection.mutable.HashMap[ScrayUUID, ScrayTServiceEndpoint]()

  def expiresFromNow = EXPIRATION.fromNow.inNanoseconds

  def createID = { val id = UUID.randomUUID(); ScrayUUID(id.getLeastSignificantBits, id.getMostSignificantBits) }

  def removeExpiredEndpoints = endpoints.values.filter(ep => Time(ep.expires.get) < Time.now)
    .foreach { ep => logger.debug(s"Removing expired endpoint ${ep.host}:${ep.port}"); endpoints.remove(ep.endpointId.get) }

  /**
   * Fetch a list of service endpoints.
   * Each endpoint provides ScrayStatelessTService and ScrayStatefulTService alternatives.
   * Queries can address different endpoints for load distribution.
   */
  def getServiceEndpoints(): Future[Seq[ScrayTServiceEndpoint]] = {
    logger.info("Meta service request: 'getServiceEndpoints'")
    removeExpiredEndpoints
    Future.value(endpoints.values.toSeq)
  }

  /**
   * Add new service endpoint.
   * The endpoint will be removed after a default expiration period.
   */
  def addServiceEndpoint(endpoint: ScrayTServiceEndpoint): Future[ScrayTServiceEndpoint] = {
    logger.info(s"Meta service request: 'addServiceEndpoint' with endpoint=$endpoint")
    val _ep = endpoint.copy(endpointId = Some(createID), expires = Some(expiresFromNow))
    endpoints.put(_ep.endpointId.get, _ep)
    Future.value(_ep)
  }

  /**
   * Restore the default expiration period of an endpoint.
   */
  def refreshServiceEndpoint(endpointID: ScrayUUID): Future[Unit] = {
    logger.info(s"Meta service request: 'refreshServiceEndpoint' with endpointID=$endpointID")
    if (endpoints.contains(endpointID))
      endpoints.update(endpointID, endpoints.get(endpointID).get.copy(expires = Some(expiresFromNow)))
    Future.value()
  }

  /**
   * Return vital sign
   */
  def ping(): Future[Boolean] = { logger.info("Meta service request: 'ping'"); Future.value(true) }

  /**
   * Shutdown the server
   */
  def shutdown(waitNanos: Option[Long]): Future[Unit] = {
    val DEFAULT_SHUTDOWN_TIMEOUT = Duration.fromSeconds(10).fromNow
    logger.info(s"Meta service request: 'shutdown' with waitNanos=$waitNanos")
    val waitUntil = waitNanos.map(Duration.fromNanoseconds(_).fromNow).getOrElse(DEFAULT_SHUTDOWN_TIMEOUT)
    new JavaTimer(false).schedule(waitUntil)(System.exit(0))
    Future.value()
  }

}
