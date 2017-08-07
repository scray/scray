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

package scray.core.service.spools

import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.ReadWriteLock
import java.util.UUID
import org.slf4j.LoggerFactory
import scala.collection.mutable.HashMap
import com.twitter.concurrent.Spool
import com.twitter.util.Duration
import com.twitter.util.Time
import com.twitter.util.TimerTask
import com.twitter.util.JavaTimer
import scray.querying.Query
import scray.querying.description.Row
import scray.service.qmodel.thrifscala.ScrayUUID
import scray.service.qmodel.thrifscala.ScrayTQueryInfo
import scray.core.service._
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
 * SpoolRack implementation offering some means to substitute the back end (query engine).
 *
 * Uses reentrant read write lock to safeguard concurrent modifications.
 *
 * Spool garbage collection controlled explicitly by task management.
 *
 * @param ttl time to live for query result sets
 * @param planAndExecute function connecting the query engine
 */
class TimedSpoolRack(val ttl: Duration = DEFAULT_TTL, planAndExecute: (Query) => Spool[Row])
  extends SpoolRack with LazyLogging {

  // internal registry, mutable, concurrency controlled
  private val spoolMap = new HashMap[UUID, (ServiceSpool, TimerTask)]()

  // spoolMap operations are locked for writing
  private final val lock: ReadWriteLock = new ReentrantReadWriteLock()

  // internal timer enforcing ttl
  private val timer = new JavaTimer(false)

  // computes expiration time for collecting frames
  private def expires = Time.now + ttl

  // monitoring wrapper for planner function
  private val wrappedPlanAndExecute: (Query) => Spool[Row] = (q) => { logger.error("wrappedPlanAndExecute"); planLog(q); planAndExecute(q) }
  private def planLog(q: Query): Unit = logger.error(s"Planner called for query $q");

  override def createSpool(query: Query, tQueryInfo: ScrayTQueryInfo): ScrayTQueryInfo = {
    
    logger.error("createSpool")
    // exit if exists
    if (spoolMap.contains(query.getQueryID)) return tQueryInfo

    // fix expiration duration
    val expiration = expires

    // pull the query id
    val quid = query.getQueryID

    //update query info
    val updQI = tQueryInfo.copy(
      queryId = Some(quid),
      expires = Some(expiration.inNanoseconds))

    // prepare this query with the engine
    val resultSpool: Spool[Row] = wrappedPlanAndExecute(query)

    // acquire write lock
    lock.writeLock().lock()

    try {

      // schedule spool removal 
      val task = timer.schedule(expiration)(removeSpool(quid))

      // add new spool
      spoolMap += (quid -> (ServiceSpool(resultSpool, updQI), task))

      // return updated query info
      updQI

    } finally {
      // finally release lock
      lock.writeLock().unlock()
    }
  }

  override def getSpool(uuid: ScrayUUID): Option[ServiceSpool] = {

    // acquire read lock
    lock.readLock().lock()

    try {

      spoolMap.get(uuid) map { tuple => tuple._1 }

    } finally {
      // finally release lock
      lock.readLock().unlock()
    }
  }

  override def updateSpool(uuid: ScrayUUID, spool: ServiceSpool): ServiceSpool = {
    
    logger.error("updateSpool")
    
    // fix expiration duration
    val expiration = expires

    //update query info
    val updSpool = ServiceSpool(spool.spool, spool.tQueryInfo.copy(expires = Some(expiration.inNanoseconds)))

    // acquire write lock
    lock.writeLock().lock()

    try {

      // cancel previous timer task if any
      spoolMap get (uuid) map { entry => entry._2.cancel() }

      // schedule spool removal 
      val task = timer.schedule(expiration)(removeSpool(uuid))

      // update the spool map
      spoolMap += (ScrayUUID2UUID(uuid) -> (updSpool, task))

      // return updated spool
      updSpool

    } finally {
      // finally release lock 
      lock.writeLock().unlock()
    }
  }

  override def removeSpool(uuid: ScrayUUID) = {

    // acquire write lock
    lock.writeLock().lock()

    try {

      // remove spool from map
      spoolMap -= uuid

    } finally {
      // finally release lock 
      lock.writeLock().unlock()
    }
  }

}
