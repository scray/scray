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
import com.twitter.concurrent.Spool.*::
import com.twitter.storehaus.QueryableStore
import com.twitter.util.{Await, Future, Throw, Return}
import com.typesafe.scalalogging.slf4j.LazyLogging
import scalax.collection.GraphEdge._
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._
import scalax.collection.immutable.Graph
import scray.querying.Registry
import scray.querying.description.{Column, Row}
import scray.querying.description.EmptyRow
import scray.querying.description.TableIdentifier
import scray.querying.description.internal.{ Domain, RangeValueDomain, Bound, SingleValueDomain }
import scray.querying.queries.DomainQuery
import scray.querying.source.SplittedAutoIndexQueryableSource.{ITERATOR_FUNCTION, WrappingIteratorRequestor}
import scray.querying.description.QueryRange
import scray.querying.source.LimitIncreasingQueryableSource.{skipIteratorEntries}
import com.twitter.util.Duration
import java.util.concurrent.TimeUnit
import scray.querying.description.internal.ComposedMultivalueDomain

/**
 * queries a Storehaus-store using a splitting algorithm. Assumes that the Seq returned by QueryableStore is a lazy sequence (i.e. view)
 */
class SplittedAutoIndexQueryableSource[K, V, T: Ordering](override val store: QueryableStore[K, V], override val space: String, table: TableIdentifier, splitcolumn: Column, rangeSplitter: Option[((T, T), Boolean) => Iterator[(T, T)]], override val isOrdered: Boolean = false) 
    extends QueryableSource[K, V](store, space, table, isOrdered) with LazyLogging {

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
  def fetchNewData: ITERATOR_FUNCTION[V, T] = (query, limit, skip, split) => Await.result {
    val ast = transformWhereAST(query.getWhereAST, split)
    val limitOpt = query.getQueryRange.map(_.copy(limit = limit)).orElse(limit.map(lim => QueryRange(None, limit)))
    val splitQuery = query.copy(domains = ast, range = limitOpt)
    logger.debug(s"re-fetch query with different range $split to fetch more results : ${splitQuery}")
    store.queryable.get(queryMapping(splitQuery)).map { x =>
      val it = x.getOrElse(Seq[V]()).view.iterator
      skip.map(count => skipIteratorEntries(count, it)).getOrElse(new Iterator[V]{
        override def hasNext: Boolean = false
        override def next: V = null.asInstanceOf[V]}
      )
    }
  }
  
  /**
   * 
   */
  def isASTContainingNonAutoIndexedColumns(query: DomainQuery): Boolean = {
    query.getWhereAST.find { domain => 
      Registry.getQuerySpaceColumn(query.getQueryspace, domain.column).flatMap { colDef =>
        colDef.index.map { index => index.isAutoIndexed && index.autoIndexConfiguration.isDefined }
      }.isEmpty
    }.isDefined
  }
  
  /**
   * delegates to an appropriate alternative source for the given query
   */
  def delegateToOtherSource(query: DomainQuery): Future[Spool[Row]] = {
    if(query.getQueryRange.isDefined && query.getQueryRange.get.limit.isDefined) {
      new LimitIncreasingQueryableSource(store, space, table, isOrdered).request(query)
    } else {
      super.request(query)
    }
  }
  
  override def request(query: DomainQuery): Future[Spool[Row]] = {
    val reversed = query.getOrdering.map(_.descending).getOrElse(false)
    logger.debug("Reversed ordering: " + reversed)
    rangeSplitter.flatMap(splitter => query.getWhereAST.find { x => x.column == splitcolumn }.map { _ match {
      case range: RangeValueDomain[T] => 
        // in this case we re-write the query to multiple queries with smaller ranges
        if(range.lowerBound.isDefined && range.upperBound.isDefined) {
          val splits = splitter((range.lowerBound.get.value, range.upperBound.get.value), reversed)
          val limitCount = query.getQueryRange.flatMap(_.limit)
          if(splits.hasNext) {
            val it = fetchNewData(query, limitCount, Some(0L), splits.next())
              // QueryableSource.iteratorToSpool[V](x.getOrElse(Seq[V]()).view.iterator, valueToRow).flatMap(_.append(query, fetchNewData, splits, limitCount))
            QueryableSource.iteratorToSpool[V](new WrappingIteratorRequestor(query, it, splits, fetchNewData, limitCount), valueToRow)
          } else {
            Future.value(Spool.empty[Row])
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

  override def getGraph: Graph[Source[DomainQuery, Spool[Row]], DiEdge] = Graph.from(List(this), List())
}

object SplittedAutoIndexQueryableSource extends LazyLogging {

  type ITERATOR_FUNCTION[V, T] = (DomainQuery, Option[Long], Option[Long], (T, T)) => Iterator[V]

  def limitIncreasingFactor = 2
  
  class WrappingIteratorRequestor[V, T](query: DomainQuery, it: Iterator[V], 
          splitIterator: Iterator[(T, T)], f: ITERATOR_FUNCTION[V, T], initialMax: Option[Long]) 
          extends Iterator[V] {
    var current = initialMax
    var max = initialMax 
    var currentIterator = it
    var lastNumberOfReturnedValues = 0L
    var currentSplit = splitIterator.next()
    
    // fetch until all splits are done
    // for each split fetch until the number of results does not exceed given limits
    
    private def fetchNext = {
      currentIterator = f(query, max.map(_ * limitIncreasingFactor), max, currentSplit)
      current = max.map(_ * (limitIncreasingFactor - 1))
      max = max.map(_ * limitIncreasingFactor)
    }
    
    override def hasNext: Boolean = currentIterator.hasNext ||  {
        if(current.isDefined && current.get == 0) {
          fetchNext
          currentIterator.hasNext
        } else {
          if(splitIterator.hasNext) {
            do {
              currentSplit = splitIterator.next()
              max = initialMax
              fetchNext
            } while(!currentIterator.hasNext && splitIterator.hasNext)
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
          fetchNext
          currentIterator.next()
        } else {
          // fetch next split, if there is one
          if(splitIterator.hasNext) {
            // watch out for empty splits -> fetch more, then
            do {
              currentSplit = splitIterator.next()
              max = initialMax
              fetchNext
            } while(!currentIterator.hasNext && splitIterator.hasNext)
            if(currentIterator.hasNext) {
              currentIterator.next()
            } else {
              null.asInstanceOf[V]
            }
          } else {
            null.asInstanceOf[V]
          }
        }
      }
    }
  }
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
  
  var configDurationName = "duration"
  var configDelimiter = " "
  var splitSize: Duration = Duration.fromSeconds(1800)
  
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

