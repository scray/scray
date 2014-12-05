package scray.core.service.spools

import java.util.UUID
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable.HashMap
import scala.util.Try
import com.twitter.concurrent.Spool
import com.twitter.util.{ Future, Time, Timer, Duration, JavaTimer }
import scray.querying.Query
import scray.querying.description.Row
import scray.querying.planning.QueryExecutor
import scray.common.exceptions.ScrayServiceException
import scray.common.exceptions.ExceptionIDs
import scray.service.qmodel.thrifscala.ScrayTQueryInfo
import com.twitter.scrooge.TFieldBlob
import scray.service.qmodel.thrifscala.ScrayTQuery
import scray.service.base.thrifscala.ScrayUUID
import scray.querying.planning.Planner
import scray.core.service.UUID2ScrayUUID

/**
 * Spool repo holding temporal query result sets
 *
 */
trait SpoolRack {

  /**
   * Add a new temporal spool to be retrieved later
   *
   * @param query the underlying query
   * @param tQueryInfo thrift meta info
   * @return updated thrift meta info to be sent back to the service client
   */
  def putSpool(query : Query, tQueryInfo : ScrayTQueryInfo) : Future[ScrayTQueryInfo]

  /**
   * Retrieve existing temporal spool for consecutively retrieving result set frames
   *
   * @param uuid query identifier
   * @return spool container holding spool and meta info
   */
  def getSpool(uuid : ScrayUUID) : Future[ServiceSpool]

  /**
   * Decommission temporal spool. This is normally called by the timer
   *
   * @param uuid query identifier
   * @return nothing
   */
  def removeSpool(uuid : UUID) : Future[Unit]
}

/**
 * Container holding spool and meta info
 */
case class ServiceSpool(val spool : Spool[Row], val tQueryInfo : ScrayTQueryInfo)

/**
 * SpoolRack singleton implementation
 */
object SpoolRack extends HashMap[UUID, ServiceSpool] with SpoolRack {

  // default time to live for query result sets
  final val TTL = Duration.fromSeconds(60)

  // timer enforcing ttl
  lazy val timer = new JavaTimer(true)

  // operations are locked for writing
  final val lock : ReadWriteLock = new ReentrantReadWriteLock()

  // we use the Planner object as entry point for the query engine
  lazy val engine = Planner

  override def putSpool(query : Query, tQueryInfo : ScrayTQueryInfo) : Future[ScrayTQueryInfo] = {
    val expires = Time.now + TTL

    //update query info
    val updQI = tQueryInfo.copy(
      queryId = Some(query.getQueryID),
      expires = Some(expires.inNanoseconds))

    // prepare this query
    val resultSpool = engine.planAndExecute(query)

    // acquire write lock
    lock.writeLock().lock()

    try {
      val result = put(query.getQueryID, ServiceSpool(resultSpool, updQI)) match {
        case Some(spool) => {
          timer.doAt(expires)(SpoolRack.removeSpool(query.getQueryID))
          Future.value(updQI)
        }
        case None => Future.exception(new ScrayServiceException(
          ExceptionIDs.SPOOLING_ERROR, Some(query.getQueryID), "Failed to store query instance.", None))
      }

      result

    } finally {
      // finally release lock 
      lock.writeLock().unlock()
    }
  }

  override def getSpool(uuid : UUID) : Future[ServiceSpool] = {

    // acquire read lock
    lock.readLock().lock()

    try {
      val result = get(uuid) match {
        case Some(spool) => Future.value(spool)
        case None => Future.exception(new ScrayServiceException(
          ExceptionIDs.SPOOLING_ERROR, Some(uuid), "Unknown query instance.", None))
      }

      result

    } finally {
      // finally release lock 
      lock.readLock().unlock()
    }
  }

  override def removeSpool(uuid : UUID) : Future[Unit] = Future.value {

    // acquire write lock
    lock.writeLock().lock()

    try { remove(uuid) } finally {
      // finally release lock 
      lock.writeLock().unlock()
    }
  }

}
