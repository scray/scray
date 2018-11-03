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
import com.twitter.util.Future
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.edge.Implicits._
import scalax.collection.GraphPredef._
import scalax.collection.GraphEdge._
import scalax.collection.immutable.Graph
import scray.querying.description.Column
import scray.querying.description.Row
import scray.querying.queries.DomainQuery
import scray.querying.caching.Cache
import scray.querying.caching.NullCache
import scray.querying.source.costs.QueryCosts
import scray.querying.source.costs.QueryCostFunctionFactory

/**
 * sources take queries and produce future results.
 * All queryable components in scray should be Sources.
 */
trait Source[Q <: DomainQuery, T] {
  
  def request(query: Q): Future[T]

  /**
   * Return estimated costs for this query in this state
   */
  def getCosts(query: Q)(implicit costFunc: QueryCostFunctionFactory): QueryCosts = costFunc.getCostFunction(this).apply(query)
  
  /**
   * the result will be comprised of a set of columns
   */
  def getColumns: Set[Column]
  
  /**
   * whether this source will return elements in the order
   * as defined in this DomainQuery
   */
  def isOrdered(query: Q): Boolean
  
  /**
   * returns a scala graph of the current setup of sources
   */
  def getGraph: Graph[Source[DomainQuery, T], DiEdge]
  
  /**
   * Returns a unique String identifying the type of source.
   * Used to identify caches.
   */
  def getDiscriminant: String
  
  /**
   * Create a cache for this source
   */
  def createCache: Cache[_]
  
  /**
   * whether this source is lazy or eager
   */
  def isLazy: Boolean
}

/**
 * a lazy Source is a component one can issue queries upon and get back
 * a Spool which contains the resulting data of type R. This should be
 * used as often as possible to prevent pulling all data into memory.
 */
trait LazySource[Q <: DomainQuery] extends Source[Q, Spool[Row]] {
  override def request(query: Q): LazyDataFuture
  override def isLazy: Boolean = true
}

/**
 * an eager Source is a component one can issue queries upon and get back
 * a Collection which contains the resulting data of type R. Everything gets
 * pulled into memory at once.
 */
trait EagerSource[Q <: DomainQuery] extends Source[Q, Seq[Row]] {
  override def request(query: Q): EagerDataFuture
  override def isLazy: Boolean = false
}

/**
 * A source that returns nothing.
 */
class NullSource[Q <: DomainQuery] extends LazySource[Q] {
  override def request(query: Q): LazyDataFuture = Future(Spool.Empty)
  override def getColumns: Set[Column] = Set()
  override def isOrdered(query: Q): Boolean = true
  override def getGraph: Graph[Source[DomainQuery, Spool[Row]], DiEdge] = Graph.empty[Source[DomainQuery, Spool[Row]], DiEdge]
  override def getDiscriminant: String = "-NullSource-"
  override def createCache: Cache[_] = new NullCache
}
