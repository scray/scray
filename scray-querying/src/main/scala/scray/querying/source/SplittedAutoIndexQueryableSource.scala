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

import com.google.common.collect.EvictingQueue
import com.twitter.concurrent.Spool
import com.twitter.concurrent.Spool.*::
import com.twitter.util.{Await, Duration, Future, FuturePool, Throw, Return}
import com.typesafe.scalalogging.slf4j.LazyLogging
import java.util.concurrent.{Executors, TimeUnit}
import scalax.collection.GraphEdge._
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._
import scalax.collection.immutable.Graph
import scray.querying.Registry
import scray.querying.description.{Column, EmptyRow, QueryRange, Row, TableIdentifier}
import scray.querying.description.internal.{ ComposedMultivalueDomain, Domain, RangeValueDomain, Bound, SingleValueDomain }
import scray.querying.queries.DomainQuery
import scray.querying.source.SplittedAutoIndexQueryableSource.{ITERATOR_FUNCTION, WrappingIteratorRequestor, EmptyIterator, limitIncreasingFactor, numberOfQueriesToPrefetch}
import scray.querying.source.LimitIncreasingQueryableSource.{skipIteratorEntries}
import java.util.concurrent.atomic.AtomicInteger
import scray.querying.source.store.QueryableStoreSource
import scray.querying.description.TableConfiguration
import scray.querying.queries.KeyedQuery

/**
 * queries a Storehaus-store using a splitting algorithm. Assumes that the Seq returned by QueryableStore is a lazy sequence (i.e. view)
 */
class SplittedAutoIndexQueryableSource[Q <: DomainQuery, T: Ordering](val store: QueryableStoreSource[Q], 
    table: TableIdentifier, tableConf: TableConfiguration[Q, _ <: DomainQuery, _], splitcolumn: Column, 
    rangeSplitter: Option[((T, T), Boolean) => Iterator[(T, T)]], val isOrdered: Boolean = false) 
    extends QueryableStoreSource[Q](table, store.getRowKeyColumns, store.getClusteringKeyColumns, store.getColumns, isOrdered) 
    with LazySource[Q] 
    with LazyLogging {

  private def transformWhereAST(domains: List[Domain[_]], bounds: (T, T)): List[Domain[_]] = {
    domains.map { domain => 
      if(domain.column == splitcolumn) {
        domain match {
          case range: RangeValueDomain[T] =>
            RangeValueDomain[T](splitcolumn, Some(Bound[T](true, bounds._1)), Some(Bound[T](false, bounds._2)), range.unequalValues)
          case _ => domain
        }
      } else {
        domain
      }
    }
  }
  
  /**
   * fetches new data re-writing the query using a given split of the range
   */
  def fetchNewData: ITERATOR_FUNCTION[Row, T] = (query, limit, skip, split) => Await.result {
    val ast = transformWhereAST(query.getWhereAST, split)
    val limitOpt = query.getQueryRange.map(_.copy(limit = limit)).orElse(limit.map(lim => QueryRange(None, limit, None)))
    val splitQuery = query.copy(domains = ast, range = limitOpt)
    logger.debug(s"re-fetch query with different range $split to fetch more results : ${splitQuery}")
    store.requestIterator(splitQuery.asInstanceOf[Q]).map { it =>
      skip.map(count => skipIteratorEntries(count, it)).getOrElse(new EmptyIterator[Row])
    }
  }
  
  /**
   * 
   */
  def isASTContainingNonAutoIndexedColumns(query: DomainQuery): Boolean = {
    query.getWhereAST.find { domain => 
      Registry.getQuerySpaceColumn(query.getQueryspace, query.querySpaceVersion, domain.column).flatMap { colDef =>
        colDef.index.map { index => index.isAutoIndexed && index.autoIndexConfiguration.isDefined }
      }.isEmpty
    }.isDefined
  }
  
  /**
   * delegates to an appropriate alternative source for the given query
   */
  def delegateToOtherSource(query: Q): Future[Iterator[Row]] = {
    if(query.getQueryRange.isDefined && query.getQueryRange.get.limit.isDefined) {
      new LimitIncreasingQueryableSource(store, tableConf, table, isOrdered).requestIterator(query)
    } else {
      store.requestIterator(query)
    }
  }

  override def request(query: Q): Future[Spool[Row]] = {
    requestIterator(query).flatMap { it => QueryableSource.iteratorToSpool[Row](it, row => row) }
  }
  
  override def keyedRequest(query: KeyedQuery): Future[Iterator[Row]] = requestIterator(query.asInstanceOf[Q])
  
  override def requestIterator(query: Q): Future[Iterator[Row]] = {
    query.queryInfo.addNewCosts {(n: Long) => {n + 42}}
    val reversed = query.getOrdering.map(_.descending).getOrElse(false)
    rangeSplitter.flatMap(splitter => query.getWhereAST.find { x => x.column == splitcolumn }.map { _ match {
      case range: RangeValueDomain[T] => 
        // in this case we re-write the query to multiple queries with smaller ranges
        if(range.lowerBound.isDefined && range.upperBound.isDefined) {
          val splits = splitter((range.lowerBound.get.value, range.upperBound.get.value), reversed)
          val limitCount = query.getQueryRange.flatMap(_.limit)
          if(splits.hasNext) {
            val prefetcher = new PrefetchingSplitIterator[Row, T](query, fetchNewData, splits, limitCount, limitIncreasingFactor, numberOfQueriesToPrefetch.get)
            val it = prefetcher.next()
              // QueryableSource.iteratorToSpool[V](x.getOrElse(Seq[V]()).view.iterator, valueToRow).flatMap(_.append(query, fetchNewData, splits, limitCount))
            Future.value(new WrappingIteratorRequestor(query, it, splits, fetchNewData, limitCount))
          } else {
            Future.value(new EmptyIterator[Row]())
          }
        } else {
          delegateToOtherSource(query) 
        }
      case single: SingleValueDomain[_] => delegateToOtherSource(query)
      case composed: ComposedMultivalueDomain[_] => delegateToOtherSource(query)
    }}).getOrElse {
      // no splitter?! --> use QueryableSource
      delegateToOtherSource(query)
    }    
  }
  
  override def getGraph: Graph[Source[DomainQuery, Spool[Row]], DiEdge] = Graph.from(List(this.asInstanceOf[Source[DomainQuery, Spool[Row]]]), List())
}

object SplittedAutoIndexQueryableSource extends LazyLogging {

  type ITERATOR_FUNCTION[V, T] = (DomainQuery, Option[Long], Option[Long], (T, T)) => Iterator[V]

  def limitIncreasingFactor = 2
  
  val numberOfQueriesToPrefetch = new AtomicInteger(5)
  
  class EmptyIterator[V] extends Iterator[V] {
    override def hasNext: Boolean = false
    override def next: V = null.asInstanceOf[V]
  }
  
  class WrappingIteratorRequestor[V, T](query: DomainQuery, it: Iterator[V], 
          splitIterator: Iterator[(T, T)], f: ITERATOR_FUNCTION[V, T], initialMax: Option[Long],
          prefetcher: Option[PrefetchingSplitIterator[V, T]] = None) 
          extends Iterator[V] {
    private var current = initialMax
    private var max = initialMax 
    private var currentIterator = it
    private var lastNumberOfReturnedValues = 0L
    private var currentSplit = splitIterator.next()
    
    // fetch until all splits are done
    // for each split fetch until the number of results does not exceed given limits
    
    private def fetchNext(fetchPrefetch: Boolean = false) = {
      val skipParam = if(fetchPrefetch) {
        Some(0L)
      } else {
        max
      }
      currentIterator = if(fetchPrefetch) {
        prefetcher.map { prefetch =>
          val itres = prefetch.next()
          currentSplit = prefetch.getCurrentSplit
          itres
        }.getOrElse(f(query, max.map(_ * limitIncreasingFactor), skipParam, currentSplit))
      } else {
        f(query, max.map(_ * limitIncreasingFactor), skipParam, currentSplit)
      }
      current = max.map(_ * (limitIncreasingFactor - 1))
      if(fetchPrefetch) {
        current = Some(0L)
      } else {
        current = max.map(_ * (limitIncreasingFactor - 1))        
      }
      max = max.map(_ * limitIncreasingFactor)
    }
    
    override def hasNext: Boolean = currentIterator.hasNext ||  {
        if(current.isDefined && current.get == 0) {
          fetchNext()
          currentIterator.hasNext
        } else {
          if((prefetcher.isDefined && prefetcher.get.hasNext) || splitIterator.hasNext) {
            do {
              max = initialMax
              prefetcher.getOrElse(currentSplit = splitIterator.next())
              fetchNext(true)
            } while(!currentIterator.hasNext && ((prefetcher.isDefined && prefetcher.get.hasNext) || splitIterator.hasNext))
            currentIterator.hasNext
          } else {
            false
          }
        }
      }
      
    override def next(): V = {
      if(currentIterator.hasNext) {
        try {
          currentIterator.next()
        } finally {
          current = current.map(_ - 1)
        }
      } else {
        if(current.isDefined && current.get == 0) {
          // watch for exact limit matches...
          fetchNext()
          currentIterator.next()
        } else {
          // fetch next split, if there is one
          if((prefetcher.isDefined && prefetcher.get.hasNext) || splitIterator.hasNext) {
            // watch out for empty splits -> fetch more, then
            do {
              max = initialMax
              prefetcher.getOrElse(currentSplit = splitIterator.next())
              fetchNext(true)
            } while(!currentIterator.hasNext && ((prefetcher.isDefined && prefetcher.get.hasNext) || splitIterator.hasNext))
            if(currentIterator.hasNext) {
              current = current.map(_ - 1)
              currentIterator.next()
            } else {
              current = Some(0L)
              null.asInstanceOf[V]
            }
          } else {
            current = Some(0L)
            null.asInstanceOf[V]
          }
        }
      }
    }
  }
}

/**
 * Iterator that executes queries in advance and in parallel, such that results can be more
 * efficiently inserted into the spool.
 * This is not thread-safe! 
 */
class PrefetchingSplitIterator[V, T](query: DomainQuery, fetch: SplittedAutoIndexQueryableSource.ITERATOR_FUNCTION[V, T],
        splitIterator: Iterator[(T, T)], max: Option[Long], limitIncreasingFactor: Int, prefetchSize: Int = 5) extends Iterator[Iterator[V]] {
  private val queue = EvictingQueue.create[Future[Iterator[V]]](prefetchSize)
  private val threadPool = FuturePool(Executors.newFixedThreadPool(prefetchSize))
  private var currentSplit: Option[(T, T)] = None
  
  def getCurrentSplit: (T, T) = currentSplit.getOrElse(splitIterator.next())
  
  private def fillQueue(): Unit = {
    if((queue.size() < prefetchSize) && splitIterator.hasNext) {
      queue.add(threadPool {
        currentSplit = Some(splitIterator.next)
        fetch(query, max.map(_ * limitIncreasingFactor), Some(0L), currentSplit.get)
      })
    }
  }
  
  private def fetchElementFromQueue(): Iterator[V] = {
    fillQueue()
    if(queue.isEmpty()) {
      new SplittedAutoIndexQueryableSource.EmptyIterator[V]
    } else {
      Await.result(queue.poll())
    }
  }
  
  override def hasNext: Boolean = {
    fillQueue()
    queue.isEmpty()
  }
  
  override def next: Iterator[V] = fetchElementFromQueue()
}


/**
 * slice a given range into pieces which can be eaten by SplittedAutoIndexQueryableSource
 */
trait Splitter[T] {
  def readConfig(config: Map[String, String]): Unit
  def getConfig: Map[String, String]
  def splitter: ((T, T), Boolean) => Iterator[(T, T)]
}

/**
 * slices a time interval given in milliseconds into equally sized durations
 */
class SimpleLinearTimeBasedSplitter extends Splitter[Long] {
  
  private var configDurationName = "duration"
  private var configDelimiter = " "
  private var splitSize: Duration = Duration.fromSeconds(1800)
  
  override def readConfig(config: Map[String, String]): Unit = config.get(configDurationName).foreach { str => 
    val arr = str.split(configDelimiter)
    splitSize = Duration(arr(0).toLong, TimeUnit.valueOf(arr(1)))
  }
  override def getConfig: Map[String, String] = Map[String, String]((configDurationName, splitSize.inNanoseconds.toString()))
  
  class SimpleTimeIterator(a: Long, b: Long, splitms: Long, reverse: Boolean) extends Iterator[(Long, Long)] {
    var last = if(reverse) b else a
    override def hasNext: Boolean = reverse match {
        case false => if(last < b) {
            true
          } else {
            false
          }
        case true => if(last > a) {
            true
          } else {
            false
          } 
      }
    override def next: (Long, Long) = {
      if(reverse) {
        val result = (a.max(last - splitms), last)
        last = a.max(last - splitms)
        result
      } else {
        val result = (last, b.min(last + splitms))
        last = b.min(last + splitms)
        result
      }
    }
  }
  
  override def splitter: ((Long, Long), Boolean) => Iterator[(Long, Long)] = (range, reverse) => {
    val splitms = splitSize.inUnit(TimeUnit.MILLISECONDS)
    val a = range._1.min(range._2)
    val b = range._1.max(range._2)    
    new SimpleTimeIterator(a, b, splitms, reverse)    
  }
}

