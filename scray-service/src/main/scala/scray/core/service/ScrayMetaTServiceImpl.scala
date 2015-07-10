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
import java.net.{ InetAddress, InetSocketAddress };
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.twitter.util.{ Future, Time, Duration, JavaTimer }
import scray.service.qservice.thrifscala.{ ScrayTServiceEndpoint, ScrayMetaTService }
import scray.service.qmodel.thrifscala.ScrayUUID

// For alternative concurrent map implementation
// import java.util.concurrent.ConcurrentHashMap
// import scala.collection.JavaConversions._

object ScrayMetaTServiceImpl extends ScrayMetaTService[Future] with LazyLogging {
  val REQUESTLOGPREFIX = "Received meta service request."

  case class ScrayServiceEndpoint(addr: InetSocketAddress, expires: Time)

  private val endpoints: scala.collection.concurrent.Map[UUID, ScrayServiceEndpoint] =
    // Alternative concurrent map implementation: ConcurrentHashMap[ScrayUUID, ScrayTServiceEndpoint]
    new scala.collection.concurrent.TrieMap[UUID, ScrayServiceEndpoint]

  /**
   *  Remove expired endpoints.
   *  The function is being triggered by a timer thread and applies CAS semantics.
   */
  def removeExpiredEndpoints = endpoints.iterator.filter { case (id, ep) => ep.expires < Time.now }
    .foreach {
      case (id, ep) =>
        endpoints.remove(id, ep) ? { logger.warn(s"Removed expired endpoint ${ep.addr}") }
    }

  implicit def sep2tep(ep: (UUID, ScrayServiceEndpoint)): ScrayTServiceEndpoint = {
    ScrayTServiceEndpoint(
      ep._2.addr.getHostString,
      ep._2.addr.getPort,
      Some(ep._1),
      Some(ep._2.expires.sinceEpoch.inMillis))
  }

  /**
   * Fetch a list of service endpoints.
   * Each endpoint provides ScrayStatelessTService and ScrayStatefulTService alternatives.
   * Queries can address different endpoints for load distribution.
   */
  def getServiceEndpoints(): Future[Seq[ScrayTServiceEndpoint]] = {
    logger.trace(REQUESTLOGPREFIX + " Operation='getServiceEndpoints'")
    removeExpiredEndpoints
    Future.value(endpoints.iterator.map[ScrayTServiceEndpoint] { ep => ep } toSeq)
  }

  implicit def tep2sep(tep: ScrayTServiceEndpoint): ScrayServiceEndpoint = new ScrayServiceEndpoint(
    new InetSocketAddress(InetAddress.getByName(tep.host), tep.port), EXPIRATION.fromNow)

  /**
   * Add new service endpoint.
   * The endpoint will be removed after a default expiration period.
   */
  def addServiceEndpoint(tEndpoint: ScrayTServiceEndpoint): Future[ScrayTServiceEndpoint] = {
    val ep: (UUID, ScrayServiceEndpoint) = (UUID.randomUUID() -> tEndpoint)
    logger.info(REQUESTLOGPREFIX + s" Operation='addServiceEndpoint' with address=${ep._2.addr} expiring at ${ep._2.expires}.")
    // we'll always add the endpoint regardless of redundancy (since we have an 'auto clean' feature)
    endpoints.put(ep._1, ep._2)
    Future.value(ep)
  }

  /**
   * Restore the default expiration period of an endpoint.
   */
  def refreshServiceEndpoint(endpointID: ScrayUUID): Future[Unit] = {
    logger.trace(REQUESTLOGPREFIX + s" Operation='refreshServiceEndpoint' with endpointID=$endpointID")
    endpoints.get(endpointID) match {
      // refresh w/ CAS semantics
      case Some(_ep) => endpoints.replace(endpointID, _ep, _ep.copy(expires = EXPIRATION.fromNow))
      case None      =>
    }
    Future.value()
  }

  /**
   * Return vital sign.
   */
  def ping(): Future[Boolean] = { logger.trace(REQUESTLOGPREFIX + " Operation='ping'"); Future.value(true) }

  /**
   * Shutdown the server.
   */
  def shutdown(waitNanos: Option[Long]): Future[Unit] = {
    val DEFAULT_SHUTDOWN_TIMEOUT = Duration.fromSeconds(10).fromNow
    logger.warn(s"Meta service request: 'shutdown' with waitNanos=$waitNanos")
    val waitUntil = waitNanos.map(Duration.fromNanoseconds(_).fromNow).getOrElse(DEFAULT_SHUTDOWN_TIMEOUT)
    new JavaTimer(false).schedule(waitUntil)(System.exit(0))
    Future.value()
  }

}
