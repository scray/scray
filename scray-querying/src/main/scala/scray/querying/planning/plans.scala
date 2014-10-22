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
package scray.querying.planning

import com.twitter.concurrent.Spool
import com.twitter.util.Future
import scray.querying.description.Row
import scray.querying.queries.DomainQuery
import scray.querying.source.{LazySource, LazyData, NullSource, Source}
import scalax.collection.io.dot.DotRootGraph
import scalax.collection.io.dot.DotGraph
import scalax.collection.io.dot.DotEdgeStmt
import scalax.collection.Graph
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.edge.LDiEdge
import scalax.collection.io.dot._
import scray.querying.description.ColumnOrdering

/**
 * a plan is used to execute a query internally
 */
sealed trait Plan[Q <: DomainQuery, T] {
  
  /**
   *  if this Plan needs in-order-merging
   */ 
  def needToOrder: Boolean
  
  /**
   * return the source to execute the query
   */
  def getSource: Source[Q, T]
  
  /**
   * writes a simplification of the plan layout for debugging
   */
  def getDot(queryId: String): String 
  
}

/**
 * a plan with a source that can be replaced, by a different one to allow
 * creating query pipelines incrementally 
 */
abstract class ComposablePlan[Q <: DomainQuery, T](source: Source[Q, T]) extends Plan[Q, T] {
  def map[Q1 <: DomainQuery, T1](source: Source[Q1, T1]): ComposablePlan[Q1, T1]
  override def getSource: Source[Q, T] = source
  override def getDot(queryId: String): String = {
    val dotGraph = DotRootGraph(directed = true, id = Some(s""""Plan for query $queryId""""))
    val edgeTransformer: Graph[Source[DomainQuery, T], DiEdge]#EdgeT => Option[(DotGraph, DotEdgeStmt)] = 
      (innerEdge: Graph[Source[DomainQuery, T], DiEdge]#EdgeT) => {
      val edge = innerEdge.edge
      Some(dotGraph, DotEdgeStmt(edge.from.toString, edge.to.toString, Nil))
    }
    source.getGraph.toDot(dotGraph, edgeTransformer)
  } 
}

object ComposablePlan {
  def getComposablePlan[Q <: DomainQuery, T](source: Source[Q, T], query: DomainQuery): ComposablePlan[Q, T] = {
    if(query.getOrdering.isDefined) { 
      new OrderedComposablePlan(source, query.getOrdering)
    } else { 
      new UnorderedComposablePlan(source)
    }
  }
}

/**
 * a plan that doesn't care about ordering of elements
 */
class UnorderedComposablePlan[Q <: DomainQuery, T](source: Source[Q, T]) extends ComposablePlan[Q, T](source) {
  override def map[Q1 <: DomainQuery, T1](source: Source[Q1, T1]): ComposablePlan[Q1, T1] =
    new UnorderedComposablePlan[Q1, T1](source)
  override def needToOrder: Boolean = false
}

/**
 * A plan that returns ordered elements. May need to be joined
 * with other ordered plans. 
 */
class OrderedComposablePlan[Q <: DomainQuery, T](source: Source[Q, T], val ordering: Option[ColumnOrdering[_]]) extends ComposablePlan[Q, T](source) {
  override def map[Q1 <: DomainQuery, T1](source: Source[Q1, T1]): ComposablePlan[Q1, T1] =
    new OrderedComposablePlan[Q1, T1](source, ordering)  
  override def needToOrder: Boolean = true
}

/**
 * NullPlan is a no strategy, i.e. on execution nothing will be done.
 * Used for testing.
 */
class NullPlan[Q <: DomainQuery] extends Plan[Q, LazyData] {
  override def needToOrder: Boolean = false
  override def getSource: Source[Q, LazyData] = (new NullSource[Q]).asInstanceOf[Source[Q, LazyData]]
  override def getDot(queryId: String): String = {
    val root = DotRootGraph(directed = true, id = Some(s""""Plan for query $queryId""""))
    Graph.empty[Source[DomainQuery, Spool[Row]], DiEdge].toDot(root, _ => None)
  }
}
