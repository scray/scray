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
package scray.querying.queries

import java.util.concurrent.atomic.AtomicLong
import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scray.querying.description.TableIdentifier
import scray.querying.description.Clause
import scala.collection.mutable.MutableList

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
