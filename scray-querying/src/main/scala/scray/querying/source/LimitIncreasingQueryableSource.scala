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
import scray.querying.caching.NullCache
import com.twitter.util.Await
import scala.annotation.tailrec
import LimitIncreasingQueryableSource.{ITERATOR_EXTENDER_FUNCTION, WrappingIteratorExtender, skipIteratorEntries}
import com.typesafe.scalalogging.LazyLogging
import scray.querying.source.store.QueryableStoreSource
import scray.querying.description.TableConfiguration
import scray.querying.queries.KeyedQuery

/**
 * queries a store. Assumes that the Seq returnes by QueryableStore is a lazy sequence (i.e. view)
 */
class LimitIncreasingQueryableSource[Q <: DomainQuery](val store: QueryableStoreSource[Q], tableConf: TableConfiguration[Q, _ <: DomainQuery, _], 
    table: TableIdentifier, val isOrdered: Boolean = false) 
    extends QueryableStoreSource[Q](table, store.getRowKeyColumns, store.getClusteringKeyColumns, store.getColumns, isOrdered)
    with LazySource[Q]
    with LazyLogging {
  
  /**
   * create function to fetch new data with, which uses a given limit
   */
  def fetchAndSkipDataIterator: ITERATOR_EXTENDER_FUNCTION[Row] = (query, limit, skip) => Await.result {
    val increasedLimitQuery = query.copy(range = Some(query.getQueryRange.get.copy(limit = Some(limit))))
    logger.debug(s"re-fetch query with increased limit $limit to fetch more results : ${increasedLimitQuery}")
    store.requestIterator(increasedLimitQuery.asInstanceOf[Q]).map { it =>
      skipIteratorEntries(skip, it)
    }
  }

  override def request(query: Q): Future[Spool[Row]] = requestIterator(query).flatMap { it =>
    QueryableSource.iteratorToSpool[Row](it, row => row)
  }

  override def keyedRequest(query: KeyedQuery): Future[Iterator[Row]] = requestIterator(query.asInstanceOf[Q])
  
  override def requestIterator(query: Q): Future[Iterator[Row]] = {
    query.queryInfo.addNewCosts {(n: Long) => {n + 42}}
    logger.debug(s"Requesting data from store with LimitIncreasingQueryableSource on query ${query}")
    store.requestIterator(query).map { it =>
        // construct lazy spool
        query.getQueryRange.flatMap { range =>
          range.limit.map { limit =>
            new WrappingIteratorExtender(query, it, fetchAndSkipDataIterator, limit)
          }
        }.getOrElse {
          it
        }
    }
    
  }
  
  override def getGraph: Graph[Source[DomainQuery, Spool[Row]], DiEdge] = Graph.from(List(this.asInstanceOf[Source[DomainQuery, Spool[Row]]]), List())
  
}

object LimitIncreasingQueryableSource extends LazyLogging {
  
  type ITERATOR_EXTENDER_FUNCTION[T] = (DomainQuery, Long, Long) => Iterator[T]
  
  def limitIncreasingFactor = 2

  /**
   * skips a number of entries from an Iterator
   */
  @tailrec def skipIteratorEntries[V](count: Long, iterator: => Iterator[V]): Iterator[V] = {
    if(!iterator.hasNext || count == 0) {
      iterator
    } else {
      iterator.next
      skipIteratorEntries(count - 1, iterator)
    }
  }
  
  /**
   * Iterator-Class which is able to fetch new data if the limit has been reached
   * Stack-safe variant of the above extender usable with iterators...
   * A single instance is not safe against multi-threading, i.e. each thread needs it's own instance.
   */
  class WrappingIteratorExtender[T](query: DomainQuery, it: Iterator[T], f : ITERATOR_EXTENDER_FUNCTION[T], initialMax: Long) extends Iterator[T] {
    var current = initialMax
    var max = initialMax 
    var currentIterator = it
    
    private def fetchNext = {
      currentIterator = f(query, max * limitIncreasingFactor, max)
      current = max * (limitIncreasingFactor - 1)
      max *= limitIncreasingFactor
    }
    
    def hasNext: Boolean = currentIterator.hasNext ||  {
        if(current == 0) {
          fetchNext
          currentIterator.hasNext
        } else {
          false
        }
      }
      
    def next(): T = {
      if(currentIterator.hasNext) {
        try {
          currentIterator.next()
        } finally {
          current -= 1
        }
      } else {
        if(current == 0) {
          fetchNext
          currentIterator.next()
        } else {
          null.asInstanceOf[T]
        }
      }
    }
  }
}
