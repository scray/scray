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
import com.twitter.util.{Future, Throw, Return}
import com.typesafe.scalalogging.slf4j.LazyLogging

import scalax.collection.GraphEdge._
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._
import scalax.collection.immutable.Graph
import scray.querying.Registry
import scray.querying.description.{Column, Row}
import scray.querying.description.EmptyRow
import scray.querying.description.TableIdentifier
import scray.querying.description.internal.RangeValueDomain
import scray.querying.description.internal.SingleValueDomain
import scray.querying.queries.DomainQuery

/**
 * queries a Storehaus-store using a splitting algorithm. Assumes that the Seq returned by QueryableStore is a lazy sequence (i.e. view)
 */
class SplittedAutoIndexQueryableSource[K, V, T](override val store: QueryableStore[K, V], override val space: String, table: TableIdentifier, splitcolumn: Column, rangeSplitter: Option[((T, T)) => List[T]], override val isOrdered: Boolean = false) 
    extends QueryableSource[K, V](store, space, table, isOrdered) with LazyLogging {
  
  def isASTContainingNonAutoIndexedColumns(query: DomainQuery): Boolean = {
    query.getWhereAST.find { domain => 
      Registry.getQuerySpaceColumn(query.getQueryspace, domain.column).flatMap { colDef =>
        colDef.index.map { index => index.isAutoIndexed && index.autoIndexConfiguration.isDefined }
      }.isEmpty
    }.isDefined
  }
  
  override def request(query: DomainQuery): Future[Spool[Row]] = {
//    @tailrec def increaseQueryLimit(limit: Long, ) = {
//      query.copy(range = Some(query.getQueryRange.get.copy(limit = Some(query.getQueryRange.get.limit.get))))
//      increaseQueryLimit(limit * 2)
//    }
    // split the range, take the data you get, extract the last column value, and restart until there are no more results
    // 1. get the 
    rangeSplitter.flatMap(splitter => query.getWhereAST.find { x => x.column == splitcolumn }.map { _ match {
      case range: RangeValueDomain[_] => 
        // in this case we re-write the query to 
        range.lowerBound.map { x => x.value }
      case single: SingleValueDomain[_] => 
        // we can handle this using QueryableSource, since there is no range to split
        if(query.getQueryRange.isDefined &&
                query.getQueryRange.get.limit.isDefined &&
                isASTContainingNonAutoIndexedColumns(query)) {
          // we need to continuously increase the limit if it has not been fulfilled and if there are more filters being applied
          // assume doubling the size is enough, and that no two rows can be the same (duplicates are not possible)
          super.request(query.copy(range = Some(query.getQueryRange.get.copy(limit = Some(query.getQueryRange.get.limit.get * 2))))).map { spool =>
            var counter = 0L
            var currentLimit = query.getQueryRange.get.limit.get * 2
            var currentValue: Row = new EmptyRow()
            spool.flatMap { row =>
              if(counter == currentLimit) {
                currentLimit *= 2
                var reached = false
                super.request(query.copy(range = Some(query.getQueryRange.get.copy(limit = Some(query.getQueryRange.get.limit.get * 2))))).
                  flatMap(_.filter(actRow => reached || {reached = currentValue == actRow; true})).
                  map{actRow => counter += 1; actRow}
              } else {
                counter += 1
                currentValue = row
                Future.value(row *:: Future.value(Spool.empty[Row]))
              }
            }
          }
        } else {
          super.request(query)
        }
        
    }}).getOrElse {
      // no splitter?! --> use QueryableSource
      super.request(query)
    }
    store.queryable.get(queryMapping(query)).transform {
      case Throw(y) => Future.exception(y)
      case Return(x) => 
        // construct lazy spool
        QueryableSource.iteratorToSpool[V](x.getOrElse(Seq[V]()).view.iterator, valueToRow)
    }
  }

  override def getGraph: Graph[Source[DomainQuery, Spool[Row]], DiEdge] = Graph.from(List(this), List())
}
