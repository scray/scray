package scray.querying.planning

import com.twitter.concurrent.Spool
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.FuturePool

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import org.slf4j.LoggerFactory

import scray.querying.Query
import scray.querying.description.Row
import scray.querying.description.internal.ExecutorShutdownException

/**
 * daemon thread executing queries in a pool 
 */
class QueryExecutor(poolSize: Int = 25, timeout: Duration = Duration.fromSeconds(60)) {

  private val log = LoggerFactory.getLogger(classOf[QueryExecutor])
  
  lazy val futurePool = FuturePool(Executors.newFixedThreadPool(poolSize))
  
  var shutdownFlag = false
  
  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run: Unit = {
      def forceShutdown: Unit = {
        val droppedTasks = futurePool.executor.shutdownNow()
        log.warn(s"""Forcing shutdown of futurePool because Timeout 
          of ${timeout.inMillis} ms has been reached. 
          Dropping ${droppedTasks.size} tasks - potential query loss.""")        
      }
      futurePool.executor.shutdown()
      try {
        if (!futurePool.executor.awaitTermination(timeout.inMillis, TimeUnit.MILLISECONDS)) {
          // time's up. Forcing shutdown
          forceShutdown
        }
      } catch {
        // handle requested cancel
        case e: InterruptedException => forceShutdown
      }
    }
  })
  
  /**
   * submit a query to the engine.
   * Thread safetyness is not guarenteed (test-and-set) but suffices as it is no problem 
   * if a query squeezes through the time slot of checking and setting shutdownFlag
   */
  def submitQuery(query: Query): Future[Spool[Row]] = if(!shutdownFlag) {
    futurePool(Planner.planAndExecute(query))
  } else {
    throw new ExecutorShutdownException(query)
  }
  
  def shutdown: Unit = {
    shutdownFlag = true
    futurePool.executor.shutdown()
  }
}

object QueryExecutor {
  
  // TODO: read config params and initialize QueryExecutor with those
  
  /**
   * reference to the query runner pool
   */
  lazy val runner = new QueryExecutor
}
