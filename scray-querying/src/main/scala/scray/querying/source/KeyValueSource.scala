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

import com.twitter.util.Future
import scray.querying.Registry
import scray.querying.description.{ Column, TableIdentifier, TableConfiguration, Row }
import scalax.collection.immutable.Graph
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._
import scalax.collection.GraphEdge._
import scray.querying.queries.{ DomainQuery, KeyedQuery }
import scray.querying.caching.{ Cache, KeyValueCache }
import scray.querying.caching.serialization.{ KeyValueCacheSerializer, RegisterRowCachingSerializers }
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
 * A source that queries a Storehaus-store for a given value.
 * The query is comprised of a simple key in this case.
 */
class KeyedSource[K <: DomainQuery, V](
    tableConfiguration: TableConfiguration[_ <: DomainQuery, K, V],
    table: TableIdentifier,
    enableCaching: Boolean = true) extends EagerSource[KeyedQuery] with LazyLogging {

  // private val queryspaceTable = Registry.getQuerySpaceTable(space, version, table)
  protected val cache = Registry.getCache[Row, KeyValueCache[K, Row]](this)
  
  val valueToRow: V => Row = tableConfiguration.rowMapper

  val store = tableConfiguration.readableStore
  
  override def request(query: KeyedQuery): Future[Seq[Row]] = {
    query.queryInfo.addNewCosts {(n: Long) => {n + 42}}
    if(enableCaching) {
      cache.retrieve(query).map { value =>
        Future.value(Seq(value))
      }.getOrElse {
        store.get.keyedRequest(query).map { it =>
          if(it.hasNext) {
            val rowValue = it.next
    	      cache.put(query, rowValue)
    	      Seq[Row](rowValue)
          } else {
            Seq()
          }
        }
      }
    } else {
      store.get.keyedRequest(query).map { it => 
        if(it.hasNext) {
          Seq(it.next())
        } else {
          Seq()
        }
      }
    }
  }
  
  override def getColumns: Set[Column] = tableConfiguration.allColumns
  
  // as there is only one element this method may return true
  override def isOrdered(query: KeyedQuery): Boolean = true
  
  override def getGraph: Graph[Source[DomainQuery, Seq[Row]], DiEdge] = 
    Graph.from(List(this.asInstanceOf[Source[DomainQuery, Seq[Row]]]), List())

  override def getDiscriminant = table.toString()
  
  override def createCache: Cache[_] = {
    RegisterRowCachingSerializers()
    new KeyValueCache[Row, Row](getDiscriminant, Some(new KeyValueCacheSerializer), Some(new KeyValueCacheSerializer))
  }
}

