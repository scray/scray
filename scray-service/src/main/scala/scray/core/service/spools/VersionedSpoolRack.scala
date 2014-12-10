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

import java.util.UUID
import com.twitter.concurrent.Spool
import com.twitter.util.JavaTimer
import java.util.concurrent.locks.ReentrantReadWriteLock
import scray.service.qmodel.thrifscala.ScrayUUID
import scala.collection.mutable.HashMap
import com.twitter.util.Duration
import scray.service.qmodel.thrifscala.ScrayTQueryInfo
import java.util.concurrent.locks.ReadWriteLock
import scray.querying.Query
import com.twitter.util.Time
import scray.querying.description.Row
import scray.core.service._

/**
 * SpoolRack implementation offering some means to substitute the back end (query engine).
 *
 * Uses reentrand read write lock to safeguard concurrent modifications.
 *
 * Spool garbage collection controlled implicitly by spool revisions.
 *
 * @param ttl time to live for query result sets
 * @param planAndExecute function connecting the query engine
 */
class VersionedSpoolRack(val ttl : Duration = Duration.fromSeconds(60), planAndExecute : (Query) => Spool[Row])
  extends SpoolRack {

  // internal registry, mutable, concurrency controlled
  private val spoolMap = new HashMap[UUID, (ServiceSpool, Long)]()

  // spoolMap operations are locked for writing
  private final val lock : ReadWriteLock = new ReentrantReadWriteLock()

  // internal timer enforcing ttl
  private lazy val timer = new JavaTimer(true)

  // spool garbage collector function
  private val gcFun = (quid : UUID, revision : Long) => {
    // need to get write lock otherwise we won't be able to upgrade later
    lock.writeLock().lock()
    try {
      spoolMap.get(quid) match {
        // we only remove the spool if no update has been done
        case Some(tuple) if (tuple._2 == revision) => removeSpool(quid)
      }
    } finally {
      lock.writeLock().unlock()
    }
  }

  // computes expiration time for collecting frames
  private def expires = Time.now + ttl

  override def createSpool(query : Query, tQueryInfo : ScrayTQueryInfo) : ScrayTQueryInfo = {
    // exit if exists
    if (spoolMap.contains(query.getQueryID)) return tQueryInfo

    // fix expiration duration
    val expiration = expires

    // fix a revision token
    val revision = System.currentTimeMillis()

    // pull the query id
    val quid = query.getQueryID

    //update query info
    val updQI = tQueryInfo.copy(
      queryId = Some(quid),
      expires = Some(expiration.inNanoseconds))

    // prepare this query with the engine
    val resultSpool : Spool[Row] = planAndExecute(query)

    // acquire write lock
    lock.writeLock().lock()

    try {

      // add new spool
      spoolMap += (quid -> (ServiceSpool(resultSpool, updQI), revision))

      // schedule spool removal 
      timer.doAt(expiration)(() => gcFun(quid, revision))

      // return updated query info
      updQI

    } finally {
      // finally release lock
      lock.writeLock().unlock()
    }
  }

  override def getSpool(uuid : ScrayUUID) : Option[ServiceSpool] = {

    // acquire read lock
    lock.readLock().lock()

    try {

      spoolMap.get(uuid) map { tuple => tuple._1 }

    } finally {
      // finally release lock
      lock.readLock().unlock()
    }
  }

  override def updateSpool(uuid : ScrayUUID, spool : ServiceSpool) : ServiceSpool = {
    // fix expiration duration
    val expiration = expires

    // fix a new revision token
    val revision = System.currentTimeMillis()

    //update query info
    val updSpool = ServiceSpool(spool.spool, spool.tQueryInfo.copy(expires = Some(expiration.inNanoseconds)))

    // acquire write lock
    lock.writeLock().lock()

    try {

      // update the spool map
      spoolMap += (ScrayUUID2UUID(uuid) -> (updSpool, revision))

      // schedule spool removal 
      timer.doAt(expiration)(() => gcFun(uuid, revision))

      // return updated spool
      updSpool

    } finally {
      // finally release lock 
      lock.writeLock().unlock()
    }
  }

  override def removeSpool(uuid : ScrayUUID) = {

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
