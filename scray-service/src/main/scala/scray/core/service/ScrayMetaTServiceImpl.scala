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

import com.twitter.util.{Future, Time, Duration, JavaTimer}
import com.typesafe.scalalogging.slf4j.LazyLogging

import java.net.{InetAddress, InetSocketAddress}
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable.HashMap

import scray.service.qmodel.thrifscala.ScrayUUID
import scray.service.qservice.thrifscala.{ScrayTServiceEndpoint, ScrayMetaTService}

// For alternative concurrent map implementation
// import java.util.concurrent.ConcurrentHashMap
// import scala.collection.JavaConversions._

object ScrayMetaTServiceImpl extends ScrayMetaTService[Future] with LazyLogging {
  val REQUESTLOGPREFIX = "Received meta service request."

  case class ScrayServiceEndpoint(addr: InetSocketAddress, expires: Time)

  private val lock = new ReentrantLock
  
  private val endpoints =
    // Alternative concurrent map implementation: ConcurrentHashMap[ScrayUUID, ScrayTServiceEndpoint]
    new HashMap[UUID, ScrayServiceEndpoint]

  /**
   *  Remove expired endpoints.
   *  The function is being triggered by a timer thread and applies CAS semantics.
   */
  def removeExpiredEndpoints = {
    lock.lock()
    try {
      val localHashMap = new HashMap ++ endpoints
      localHashMap.filter{ case (id, ep) => ep.expires < Time.now }.foreach {
          case (id, ep) =>
              endpoints.remove(id).map(_ => logger.warn(s"Removed expired endpoint ${ep.addr}"))        
      }
    } finally {
      lock.unlock()
    }
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
    lock.lock
    try {
      logger.info(REQUESTLOGPREFIX + " Operation='getServiceEndpoints'")
      removeExpiredEndpoints
      Future.value(endpoints.iterator.map[ScrayTServiceEndpoint] { ep => ep } toSeq)
    } finally {
      lock.unlock()
    }
  }

  implicit def tep2sep(tep: ScrayTServiceEndpoint): ScrayServiceEndpoint = new ScrayServiceEndpoint(
    new InetSocketAddress(InetAddress.getByName(tep.host), tep.port), EXPIRATION.fromNow)

  /**
   * Add new service endpoint.
   * The endpoint will be removed after a default expiration period.
   */
  def addServiceEndpoint(tEndpoint: ScrayTServiceEndpoint): Future[ScrayTServiceEndpoint] = {
    val ep: (UUID, ScrayServiceEndpoint) = (UUID.randomUUID() -> tEndpoint)
    lock.lock()
    try {
      logger.info(REQUESTLOGPREFIX + s" Operation='addServiceEndpoint' with address=${ep._2.addr} expiring at ${ep._2.expires}.")
      // we'll always add the endpoint regardless of redundancy (since we have an 'auto clean' feature)
      endpoints.iterator.find { p => p._2.addr.getHostString == tEndpoint._1 }.map{ p =>
        endpoints.put(p._1, p._2.copy(expires = EXPIRATION.fromNow))
        Future.value(sep2tep(p)) 
      }.getOrElse {
        endpoints.put(ep._1, ep._2)    
        Future.value(ep)
      }
    } finally {
      lock.unlock()
    }
  }

  /**
   * Restore the default expiration period of an endpoint.
   */
  def refreshServiceEndpoint(endpointID: ScrayUUID): Future[Unit] = {
    lock.lock()
    try {
      logger.info(REQUESTLOGPREFIX + s" Operation='refreshServiceEndpoint' with endpointID=$endpointID")
      endpoints.get(endpointID) match {
        // refresh w/ CAS semantics
        case Some(_ep) => endpoints.put(endpointID, _ep.copy(expires = EXPIRATION.fromNow))
        case None      => 
      }
      Future.value()
    } finally {
      lock.unlock()
    }
  }

  /**
   * Return vital sign.
   */
  def ping(): Future[Boolean] = { logger.info(REQUESTLOGPREFIX + " Operation='ping'"); Future.value(true) }

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
