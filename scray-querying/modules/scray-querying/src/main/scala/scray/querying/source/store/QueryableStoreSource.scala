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
package scray.querying.source.store

import scray.querying.source.LazySource
import scray.querying.queries.DomainQuery
import scray.querying.caching.NullCache
import scray.querying.caching.Cache
import scray.querying.description.Column
import scray.querying.description.TableIdentifier
import com.twitter.concurrent.Spool
import scray.querying.description.Row
import scalax.collection.immutable.Graph
import scray.querying.source.Source
import scalax.collection.GraphEdge.DiEdge
import com.twitter.util.Future
import scray.querying.description.RowColumn
import scray.querying.queries.KeyedQuery

/**
 * super-class of 
 */
abstract class QueryableStoreSource[Q <: DomainQuery](
    ti: TableIdentifier,
    rowKeyColumns: Set[Column], 
    clusteringKeyColumns: Set[Column],
    allColumns: Set[Column],
    ordered: Boolean) extends LazySource[Q] {
  val valueColumns = allColumns -- rowKeyColumns -- clusteringKeyColumns
  
  /**
   * As seen from class QueryableStore, the missing signatures are as follows.
   *  For convenience, these are usable as stub implementations.
   */
  // Members declared in scray.querying.source.LazySource
  override def request(query: Q): scray.querying.source.LazyDataFuture
  def requestIterator(query: Q): Future[Iterator[Row]]
  def keyedRequest(query: KeyedQuery): Future[Iterator[Row]]
  
  def hasSkipAndLimit: Boolean = false
  
  def getScrayCoordinates: TableIdentifier = ti
  
  // Members declared in scray.querying.source.Source
  def createCache: Cache[_] = new NullCache
  
  def getColumns: Set[Column] = allColumns
  def getRowKeyColumns: Set[Column] = rowKeyColumns
  def getClusteringKeyColumns: Set[Column] = clusteringKeyColumns
  def getValueColumns: Set[Column] = valueColumns
  
  def getDiscriminant: String = {
    val tid = getScrayCoordinates
    s"${tid.dbSystem}.${tid.dbId}.${tid.tableId}"
  }
  def getGraph: Graph[Source[DomainQuery, Spool[Row]], DiEdge] = Graph.from(List(this.asInstanceOf[LazySource[DomainQuery]]), List())
  def isOrdered(query: Q): Boolean = ordered
}
