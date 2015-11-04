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

import com.twitter.concurrent.Spool
import com.twitter.storehaus.{IterableStore, QueryableStore}
import com.twitter.util.{Future, Throw, Return}
import scray.querying.description.{Column, Row}
import scray.querying.queries.DomainQuery
import scray.querying.Registry
import scray.querying.description.TableIdentifier
import scalax.collection.immutable.Graph
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._
import scalax.collection.GraphEdge._
import scray.querying.caching.Cache
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.querying.caching.NullCache

/**
 * queries a Storehaus-store. Assumes that the Seq returnes by QueryableStore is a lazy sequence (i.e. view)
 */
class QueryableSource[K, V](val store: QueryableStore[K, V], val space: String, val version: Int, table: TableIdentifier, val isOrdered: Boolean = false) 
    extends LazySource[DomainQuery] with LazyLogging {

  protected val queryspaceTable = Registry.getQuerySpaceTable(space, version, table) 
  
  val valueToRow: (V) => Row = queryspaceTable.get.rowMapper.asInstanceOf[(V) => Row]
  
  val queryMapping: DomainQuery => K = queryspaceTable.get.domainQueryMapping.asInstanceOf[DomainQuery => K]
  
  override def request(query: DomainQuery): Future[Spool[Row]] = {
    logger.debug(s"Requesting data from store with ${query.getQueryID}")
    store.queryable.get(queryMapping(query)).transform {
      case Throw(y) => Future.exception(y)
      case Return(x) => 
        // construct lazy spool
        QueryableSource.iteratorToSpool[V](x.getOrElse(Seq[V]()).view.iterator, valueToRow)
    }
  }

  override def getColumns: List[Column] = queryspaceTable.get.allColumns
  
  /**
   * looks up in the registry if we can fulfill the ordering
   */
  override def isOrdered(query: DomainQuery): Boolean = {
    isOrdered || (query.getOrdering match {
      case Some(col) => Registry.getQuerySpaceColumn(space, query.querySpaceVersion, col.column) match {
          case None => false
          case Some(colConfig) => colConfig.index.map(_.isSorted).getOrElse(false)
        }
      case None => false
    })
  }
  
  override def getGraph: Graph[Source[DomainQuery, Spool[Row]], DiEdge] = Graph.from(List(this), List())
  
  override def getDiscriminant = table.toString()
  
  override def createCache: Cache[Spool[Row]] = (new NullCache).asInstanceOf[Cache[Spool[Row]]]
}

object QueryableSource {
  
  /**
   * copied from com.twitter.storehaus.IterableStore, but removed tuple dependency
   */
  def iteratorToSpool[V](it: Iterator[V], transformer: (V) => Row): Future[Spool[Row]] = Future.value {
    if (it.hasNext) {
      // *:: for lazy/deferred tail
      transformer(it.next) *:: iteratorToSpool(it, transformer)
    } else {
      Spool.empty
    }
  }
}
