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
import com.twitter.util.Await
import scala.annotation.tailrec

/**
 * queries a Storehaus-store. Assumes that the Seq returnes by QueryableStore is a lazy sequence (i.e. view)
 */
class LimitIncreasingQueryableSource[K, V](override val store: QueryableStore[K, V], override val space: String, table: TableIdentifier, 
        override val isOrdered: Boolean = false, limitIncreasingFactor: Int = 2) 
    extends QueryableSource[K, V](store, space, table, isOrdered) with LazyLogging {

  /**
   * extend Spool[Row] with a method which is able to fetch new data if the limit has been reached
   */
  implicit class SpoolExtender(spool: Spool[Row]) {
    def extend(origQuery: DomainQuery, f : (DomainQuery, Long, Long) => Spool[Row], max: Long, current: Long): Future[Spool[Row]] = {
      def _tail = spool.tail flatMap (_.extend(origQuery, f, max, current - 1))
      if(spool.isEmpty) {
        if(current == 0) {
          // fetch another dataset
          f(origQuery, max * limitIncreasingFactor, max).extend(origQuery, f, max * limitIncreasingFactor, max * limitIncreasingFactor)
        } else {
          // we're finished with our query
          Future.value(Spool.empty[Row])
        }
      } else {
        Future.value(spool.head *:: _tail)
      }
    }
  }

  /**
   * create function to fetch new data with, which uses a given limit
   */
  def fetchAndSkipData: (DomainQuery, Long, Long) => Spool[Row] = (query, limit, skip) => Await.result {
    @tailrec def consumeUntil0(count: Long, spool: => Spool[Row]): Future[Spool[Row]] = {
      if(spool.isEmpty || count == 0) {
        Future.value(spool)
      } else {
        def tail = Await.result(spool.tail)
        consumeUntil0(count - 1, tail)
      }
    }
    val increasedLimitQuery = query.copy(range = Some(query.getQueryRange.get.copy(limit = Some(limit))))
    logger.debug(s"re-fetch query with increased limit $limit to fetch more results : ${increasedLimitQuery}")
    store.queryable.get(queryMapping(increasedLimitQuery)).transform {
      case Throw(y) => Future.exception(y)
      case Return(x) =>
        // construct lazy spool
        val spool = QueryableSource.iteratorToSpool[V](x.getOrElse(Seq[V]()).view.iterator, valueToRow)
        consumeUntil0(skip, Await.result(spool))
    }
  }
  
  override def request(query: DomainQuery): Future[Spool[Row]] = {
    logger.debug(s"Requesting data from store with LimitIncreasingQueryableSource on query ${query}")
    store.queryable.get(queryMapping(query)).transform {
      case Throw(y) => Future.exception(y)
      case Return(x) => 
        // construct lazy spool
        query.getQueryRange.flatMap { range =>
          range.limit.map { limit => 
            QueryableSource.iteratorToSpool[V](x.getOrElse(Seq[V]()).view.iterator, valueToRow).flatMap(_.extend(query, fetchAndSkipData, limit, limit))
          }
        }.getOrElse {
          super.request(query)
        }
    }
  }

  override def getGraph: Graph[Source[DomainQuery, Spool[Row]], DiEdge] = Graph.from(List(this), List())
  
}
