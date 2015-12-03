package scray.querying.queries

import java.util.concurrent.atomic.AtomicLong
import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scray.querying.description.TableIdentifier
import scray.querying.description.Clause

/**
 * information on queries that can be monitored
 */
class QueryInformation(val qid: UUID, val table: TableIdentifier, 
                       val where: Option[Clause], val startTime: Long = System.currentTimeMillis()) {
  
  /**
   * number of items that we have collected so far
   */
  val resultItems = new AtomicLong(0L)
  
  /**
   * last time we got updates for this query
   */
  val pollingTime = new AtomicLong(-1L)
  
  /**
   * if the query succeeds and finishes, this contains the finish time
   */
  val finished = new AtomicLong(-1L)
  
  /**
   * time when planing finished
   */
  val finishedPlanningTime = new AtomicLong(-1L)
  
  /**
   * time when data request was sent to database
   */
  val requestSentTime = new AtomicLong(-1L)
  
  private val destructionListeners = new ArrayBuffer[DESTRUCTOR] 
  
  def registerDestructionListerner(listener: DESTRUCTOR) = destructionListeners += listener 
  
  def destroy() = {
    destructionListeners.foreach(_())
    destructionListeners.clear()
  }
}
