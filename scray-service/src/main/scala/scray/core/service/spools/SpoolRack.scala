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
import scray.querying.planning.Planner
import scray.core.service._
import scala.util.Failure
import scala.util.Success

/**
 * Container holding spool and meta info
 */
case class ServiceSpool(val spool : Spool[Row], val tQueryInfo : ScrayTQueryInfo)

/**
 * SpoolRack singleton based on TimedSpoolRack
 */
object SpoolRack extends TimedSpoolRack(planAndExecute = Planner.planAndExecute)

/**
 * Spool repo holding temporal query result sets
 *
 */
trait SpoolRack {
  /**
   * Create a new temporal spool for a given queue to be retrieved later.
   *
   * This function is idempotent.
   *
   * @param query the underlying query
   * @param tQueryInfo thrift meta info
   * @return updated thrift meta info to be sent back to the service client
   */
  def createSpool(query : Query, tQueryInfo : ScrayTQueryInfo) : ScrayTQueryInfo

  /**
   * Retrieve existing temporal spool for consecutively retrieving result set frames.
   *
   * @param uuid query identifier
   * @return spool container holding spool and meta info if exists else None
   */
  def getSpool(uuid : ScrayUUID) : Option[ServiceSpool]

  /**
   * Update existing temporal spool for keeping state of delivered result set frames.
   *
   * @param uuid query identifier
   * @param spool container holding spool and meta info
   * @return updated spool container
   */
  def updateSpool(uuid : ScrayUUID, spool : ServiceSpool) : ServiceSpool

  /**
   * Decommission temporal spool.
   *
   * This function is normally called by the timer after TTL.
   *
   * @param uuid query identifier
   * @return nothing
   */
  def removeSpool(uuid : ScrayUUID) : Unit
}
