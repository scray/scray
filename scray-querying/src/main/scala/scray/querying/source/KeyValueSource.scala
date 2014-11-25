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

import scray.querying.Query
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Future
import scray.querying.queries.KeyBasedQuery
import scray.querying.description.Row
import scray.querying.Registry
import scray.querying.description.Column
import scray.querying.description.TableIdentifier
import scalax.collection.immutable.Graph
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._
import scalax.collection.GraphEdge._
import scray.querying.queries.DomainQuery
import scray.querying.caching.KeyValueCache
import scray.querying.caching.Cache
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
 * A source that queries a Storehaus-store for a given value.
 * The query is comprised of a simple key in this case.
 */
class KeyValueSource[K, V](val store: ReadableStore[K, V], 
    space: String, table: TableIdentifier, enableCaching: Boolean = true) extends EagerSource[KeyBasedQuery[K]] with LazyLogging {

  private val queryspaceTable = Registry.getQuerySpaceTable(space, table)
  private val cache = Registry.getCache[V, KeyValueCache[K, V]](this)
  
  val valueToRow: ((K, V)) => Row = queryspaceTable.get.rowMapper.asInstanceOf[((K, V)) => Row]

  override def request(query: KeyBasedQuery[K]): Future[Seq[Row]] = {
    logger.debug(s"Requesting row with key ${query.key} for query ${query.getQueryID}")
    if(enableCaching) {
      cache.retrieve(query).map { value =>
          logger.debug(s"Cache HIT for key ${query.key} for query ${query.getQueryID}")
          Future.value(Seq(valueToRow((query.key, value))))
        }.getOrElse {
          logger.debug(s"Cache MISS for key ${query.key} for query ${query.getQueryID}")
          store.get(query.key).map {
      	    case None => Seq[Row]()
      	    case Some(value) =>
      	      cache.put(query, value)
              logger.debug(s"Cache PUT for key ${query.key} for query ${query.getQueryID}")
      	      Seq(valueToRow((query.key, value)))
          }
        }
    } else {
      store.get(query.key).map {
      	case None => Seq[Row]()
      	case Some(value) =>
  	      Seq(valueToRow((query.key, value)))
      }
    }
  }
  
  override def getColumns: List[Column] = queryspaceTable.get.allColumns
  
  // as there is only one element this method may return true
  override def isOrdered(query: KeyBasedQuery[K]): Boolean = true
  
  override def getGraph: Graph[Source[DomainQuery, Seq[Row]], DiEdge] = 
    Graph.from(List(this.asInstanceOf[Source[DomainQuery, Seq[Row]]]), List())

  override def getDiscriminant = table.toString()
  
  override def createCache: Cache[_] = new KeyValueCache[K, V](getDiscriminant)
}
