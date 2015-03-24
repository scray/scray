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
import com.twitter.storehaus.QueryableStore
import com.twitter.util.{Future, Throw, Return}
import com.typesafe.scalalogging.slf4j.LazyLogging
import scalax.collection.GraphEdge._
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._
import scalax.collection.immutable.Graph
import scray.querying.description.{ Column, Row }
import scray.querying.description.internal.SingleValueDomain
import scray.querying.queries.DomainQuery
import scray.querying.planning.MergingResultSpool
import scala.annotation.tailrec

/**
 * queries a Storehaus-store with a number of parallel queries. 
 * Assumes that the Seq returnes by QueryableStore is a lazy sequence (i.e. view)
 */
class ParallelizedQueryableSource[K, V](override val store: QueryableStore[K, V], override val space: String, 
        parallelizationColumn: Column, parallelization: () => Option[Int], ordering: Option[(Row, Row) => Boolean]) extends 
        QueryableSource[K, V](store, space, parallelizationColumn.table, ordering.isDefined) with LazyLogging {
  
  @inline def getTransformedDomainQuery(query: DomainQuery, number: Int): K =
    queryMapping(query.transformedAstCopy(SingleValueDomain[Int](parallelizationColumn, number) :: query.getWhereAST))
  
  @inline def spool(query: DomainQuery, number: Int): Future[Spool[Row]] = {
    store.queryable.get(getTransformedDomainQuery(query, number)).transform {
      case Throw(y) => Future.exception(y)
      case Return(x) => 
        // construct lazy spool
        QueryableSource.iteratorToSpool[V](x.getOrElse(Seq[V]()).view.iterator, valueToRow)
    }
  }

  @tailrec private def executeQueries(query: DomainQuery, parallel: Int, spools: List[Future[Spool[Row]]]): 
      List[Future[Spool[Row]]] = if(parallel <= 0) {
    spools
  } else {
    executeQueries(query, parallel - 1, spool(query, parallel) :: spools)
  }

  override def request(query: DomainQuery): Future[Spool[Row]] = {
    parallelization().map { numberQueries => 
      logger.debug(s"Requesting data from store with ${query.getQueryID} using ${numberQueries} queries in parallel")
      val spools = executeQueries(query, numberQueries, Nil)
      isOrdered match {
        case true => MergingResultSpool.mergeOrderedSpools(Seq(), spools, ordering.get, Seq())
        case false => MergingResultSpool.mergeUnorderedResults(Seq(), spools, Seq())
      }
    }.getOrElse {
      super.request(query)
    }
  }

  override def getGraph: Graph[Source[DomainQuery, Spool[Row]], DiEdge] = Graph.from(List(this), List())
}
