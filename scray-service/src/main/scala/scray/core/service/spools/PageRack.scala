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
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable.HashMap
import scala.util.Try
import com.twitter.concurrent.Spool
import com.twitter.util.{ Future, Time, Timer, Duration, JavaTimer }
import scray.querying.Query
import scray.querying.description.Row
import scray.querying.planning.QueryExecutor
import scray.core.service.ScrayServiceException
import scray.core.service.ExceptionIDs
import scray.service.qmodel.thrifscala.ScrayTQueryInfo
import com.twitter.scrooge.TFieldBlob
import scray.service.qmodel.thrifscala.ScrayTQuery
import scray.service.qmodel.thrifscala.ScrayUUID
import scray.core.service._
import scala.util.Failure
import scala.util.Success
import scray.core.service.spools.memcached.MemcachedPageRack
import scray.querying.planning.Planner
import org.slf4j.LoggerFactory

/**
 * Page identifier
 */
case class PageKey(uuid : ScrayUUID, pageIndex : Int)

/**
 * Page container
 */
case class PageValue(page : Seq[Row], tQueryInfo : ScrayTQueryInfo)

/**
 * PageRack singleton
 */
object MPageRack extends MemcachedPageRack(planAndExecute = Planner.planAndExecute)

abstract class PageRackImplBase(val planAndExecute : (Query) => Spool[Row], val pageTTL : Duration = DEFAULT_TTL) extends PageRack {
  val logger = LoggerFactory.getLogger(classOf[PageRackImplBase])

  logger.error("PageRackImplBase")
  
  // computes expiration time for collecting frames
  def expires = Time.now + pageTTL

  // monitoring wrapper for planner function
  val wrappedPlanAndExecute : (Query) => Spool[Row] = (q) => { planLog(q); planAndExecute(q) }
  def planLog(q : Query) : Unit = logger.info(s"Planner called for query $q");
}

/**
 * Page cache holding individual pages of query result sets
 *
 */
trait PageRack {

  /**
   * Create a new temporal page set for a given queue to be retrieved later.
   *
   * @param query the underlying query
   * @param tQueryInfo thrift meta info
   * @return updated thrift meta info to be sent back to the service client
   */
  def createPages(query : Query, tQueryInfo : ScrayTQueryInfo) : ScrayTQueryInfo

  /**
   * Retrieve an existing temporal page.
   *
   * @param id page identifier
   * @return page container holding page and meta info if exists else None
   */
  def getPage(key : PageKey) : Future[Option[PageValue]]

}

