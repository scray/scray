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
 * daemon thread executing queries in a pool  */

class QueryExecutor(poolSize: Int = QueryExecutor.threadPoolSize,
      timeout: Duration = Duration.fromSeconds(QueryExecutor.numberOfSecondsForShutdown)) {

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
  
  /**
   * reference to the query runner pool
   */
  lazy val runner = new QueryExecutor
  
  // default number of processors = number of query engine threads
  val threadPoolSize = Runtime.getRuntime().availableProcessors()
  
  // default number of seconds = 1 minute
  val numberOfSecondsForShutdown = 60
}
