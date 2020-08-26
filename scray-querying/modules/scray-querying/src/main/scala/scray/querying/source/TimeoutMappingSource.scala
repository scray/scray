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
package scray.querying.source

import com.typesafe.scalalogging.LazyLogging
import scray.querying.description.Row
import scray.querying.queries.DomainQuery
import scray.querying.description.EmptyRow
import scray.querying.description.internal.SingleValueDomain
import scray.querying.description.internal.QueryTimeOutException


/**
 * Check if query timed out. Triggered by received data.
 */
class TimeoutMappingSource [Q <: DomainQuery](source: LazySource[Q]) extends LazyQueryMappingSource[Q](source) with LazyLogging {
  
  var startTime: Long = 0;
  
  override def transformSpoolElement(element: Row, query: Q): Row = {
    query.getQueryRange.filter(queryRange => queryRange.timeout  match {
      case None =>   false
      case Some(time) => {(time * 1000 + startTime) < System.currentTimeMillis()}
    }) match {
      case None => element
      case Some(x) => throw  new QueryTimeOutException(query)
    }
  }
  
  override def init(query: Q): Unit = {
    startTime = System.currentTimeMillis();
  }
  
  def getColumns: Set[scray.querying.description.Column] = { source.getColumns }
  def getDiscriminant: String = {"Timeout" + source.getDiscriminant}
}