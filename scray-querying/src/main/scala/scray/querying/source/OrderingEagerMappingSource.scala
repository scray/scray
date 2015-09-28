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

import scray.querying.queries.DomainQuery
import scray.querying.description.Column
import scray.querying.description.Row
import scray.querying.description.internal.SingleValueDomain
import scray.querying.description.internal.RangeValueDomain
import scray.querying.description.ColumnOrdering
import scray.querying.caching.Cache
import scray.querying.caching.NullCache
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
 * source which pulls all data into memory and starts sorting
 * TODO: flatten that stuff to disk and do a merge sort / TimSort on disk files
 */
class OrderingEagerMappingSource[Q <: DomainQuery, R](source: Source[Q, R])
    extends EagerCollectingQueryMappingSource[Q, R](source) with LazyLogging {
  
  @inline override def transformSeq(element: Seq[Row], query: Q): Seq[Row] = {
    logger.debug(s"Ordering output in memory for ${query}")
    val queryOrdering = query.getOrdering.get
    element.sortWith(rowCompWithOrdering(queryOrdering.column, queryOrdering.ordering, queryOrdering.descending))
  }

  override def transformSeqElement(element: Row, query: Q): Row = element

  override def getColumns: List[Column] = source.getColumns
  
  override def isOrdered(query: Q): Boolean = true
  
  override def getDiscriminant = "Ordering" + source.getDiscriminant
  
  override def createCache: Cache[Nothing] = new NullCache
}
