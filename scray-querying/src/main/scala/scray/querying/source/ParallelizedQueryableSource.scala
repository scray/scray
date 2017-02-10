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
import scalax.collection.GraphEdge._
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._
import scalax.collection.immutable.Graph
import scray.querying.description.{ Column, Row }
import scray.querying.description.internal.SingleValueDomain
import scray.querying.queries.DomainQuery
import scray.querying.planning.MergingResultSpool
import scala.annotation.tailrec
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.querying.source.store.QueryableStoreSource
import scray.querying.description.TableIdentifier
import scray.querying.caching.Cache
import scray.querying.caching.NullCache

/**
 * queries a Storehaus-store with a number of parallel queries. 
 * Assumes that the Seq returnes by QueryableStore is a lazy sequence (i.e. view)
 */
class ParallelizedQueryableSource[Q <: DomainQuery](val store: QueryableStoreSource[Q], ti: TableIdentifier, queryMapping: DomainQuery => Q,
        parallelizationColumn: Column, parallelization: Option[Int], ordering: Option[(Row, Row) => Boolean], descending: Boolean) 
        extends LazySource[Q] with LazyLogging {
  
  @inline def getTransformedDomainQuery(query: DomainQuery, number: Int): Q =
    (query.transformedAstCopy(SingleValueDomain[Int](parallelizationColumn, number) :: query.getWhereAST)).asInstanceOf[Q]
  
  @inline def spool(query: DomainQuery, number: Int): Future[Spool[Row]] = {
    store.requestIterator(getTransformedDomainQuery(query, number)).flatMap { it =>
      // construct lazy spool
      QueryableSource.iteratorToSpool[Row](it, row => row)
    }
  }

  @tailrec private def executeQueries(query: DomainQuery, parallel: Int, spools: List[Future[Spool[Row]]]): 
      List[Future[Spool[Row]]] = if(parallel <= 0) {
    spools
  } else {
    executeQueries(query, parallel - 1, spool(query, parallel) :: spools)
  }

  override def request(query: Q): Future[Spool[Row]] = {
    query.queryInfo.addNewCosts {(n: Long) => {n + 42}}
    parallelization.map { numberQueries => 
      logger.debug(s"Requesting data from store with ${query.getQueryID} using ${numberQueries} queries in parallel")
      val spools = executeQueries(query, numberQueries, Nil)
      ordering.isDefined match {
        case true => MergingResultSpool.mergeOrderedSpools(Seq(), spools, ordering.get, descending, Seq())
        case false => MergingResultSpool.mergeUnorderedResults(Seq(), spools, Seq())
      }
    }.getOrElse {
      store.request(query)
    }
  }

  /** As seen from class ParallelizedQueryableSource, the missing signatures are as follows.
   *  For convenience, these are usable as stub implementations.
   */
  def createCache: Cache[_] = new NullCache
  def getColumns: Set[Column] = store.getColumns
  def getDiscriminant: String = this.getClass.getCanonicalName + store.getDiscriminant
  def isOrdered(query: Q): Boolean = store.isOrdered(query)
  
  override def getGraph: Graph[Source[DomainQuery, Spool[Row]], DiEdge] = Graph.from(List(this.asInstanceOf[Source[DomainQuery, Spool[Row]]]), List())
}
