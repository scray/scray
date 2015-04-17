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
import scray.querying.caching.serialization.KeyValueCacheSerializer
import scray.querying.caching.serialization.RegisterRowCachingSerializers
import scray.querying.queries.{KeyBasedQuery, KeySetBasedQuery}
import scala.collection.mutable.ArrayBuffer
import com.twitter.util.Return
import com.twitter.util.Await
import com.twitter.util.Try

/**
 * A source that queries a Storehaus-store for a set of values.
 * The query is comprised of a set of keys.
 */
class ParallelizedKeyValueSource[K, V](override val store: ReadableStore[K, V], 
    space: String, table: TableIdentifier, enableCaching: Boolean = true) 
    extends KeyValueSource[K, V](store, space, table, enableCaching) with LazyLogging {

  def request(query: KeySetBasedQuery[K]): Future[Seq[Row]] = {
    if(enableCaching) {
      // retrieve values that are already cached
      val buffer = new ArrayBuffer[K]
      val cachedResults = query.getAsKeyBasedQueries().map(singlekeyquery => cache.retrieve(singlekeyquery).
             orElse{buffer += singlekeyquery.key; None}).toSeq.collect { case Some(value) => value }
      if(buffer.isEmpty) {
        // all values are already in the cache
        Future.value(cachedResults)
      } else {
        val qresults = store.multiGet(buffer.toSet)
        // handle values from the store and join them with the cachedResults
        Future.join(qresults.values.toList).map(_ => qresults.collect {
          case (key, Future(value)) if value.isReturn && value.get.isDefined => 
            // at this point all the futures will already be satisfied because we did the join
            val rowValue = valueToRow((key, value.get.get))
            cache.put(query, rowValue)
            rowValue
        }.toSeq ++ cachedResults)
      }
    } else {
      val qresults = store.multiGet(query.key)
      Future.join(qresults.values.toList).map(_ => qresults.collect {
        case (key, Future(value)) if value.isReturn && value.get.isDefined => valueToRow((key, value.get.get))
      }.toSeq)
    }
  }
  
  override def isOrdered(query: KeyBasedQuery[K]): Boolean = false
  
  override def getGraph: Graph[Source[DomainQuery, Seq[Row]], DiEdge] = 
    Graph.from(List(this.asInstanceOf[Source[DomainQuery, Seq[Row]]]), List())
}
