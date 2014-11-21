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
import com.twitter.util.{Await, Future}
import scala.annotation.tailrec
import scala.collection.mutable.ArraySeq
import scala.collection.parallel.mutable.ParArray
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._
import scalax.collection.immutable.Graph
import scray.querying.description.{Column, CompositeRow, EmptyRow, Row, TableIdentifier}
import scray.querying.planning.MergingResultSpool
import scray.querying.queries.{DomainQuery, KeyBasedQuery}
import scray.querying.description.ColumnOrdering
import scray.querying.caching.Cache
import scray.querying.caching.NullCache

/**
 * This hash joined source provides a template for implementing hashed-joins. This is a relational lookup.
 * 
 * The query Q1 of "source" yields a spool which can produce references of type R.
 * The query Q2 of "lookupSource" is then queried for R and the combined results are returned.
 * 
 * This is an inner join, but results must be filtered for EmptyRow.
 */
abstract class AbstractHashJoinSource[Q <: DomainQuery, M, R /* <: Product */, V](
    indexsource: LazySource[Q],
    lookupSource: KeyValueSource[R, V],
    lookupSourceTable: TableIdentifier,
    lookupkeymapper: Option[M => R] = None)
  extends LazySource[Q] {

  /**
   * transforms the spool and queries the lookup source.
   */
  @inline protected def spoolTransform(spool: Spool[Row], query: Q): Future[Spool[Row]] = {
    @tailrec def insertRowsIntoSpool(rows: ArraySeq[Row], spoolElements: Spool[Row]): Spool[Row] = {
      if(rows.isEmpty) {
        spoolElements
      } else {
        insertRowsIntoSpool(rows.tail, rows.head *:: Future(spoolElements))
      }
    }
    spool.flatMap { in => 
      val joinables = getJoinablesFromIndexSource(in)
      // execute this flatMap on a parallel collection
      val parseq: ParArray[Option[Row]] = joinables.par.map { lookupTuple => 
        val keyValueSourceQuery = new KeyBasedQuery[R](
          lookupkeymapper.map(_(lookupTuple)).getOrElse(lookupTuple.asInstanceOf[R]),
          lookupSourceTable,
          lookupSource.getColumns,
          query.querySpace,
          query.getQueryID)
        val seq = Await.result(lookupSource.request(keyValueSourceQuery))
        if(seq.size > 0) {
          val head = seq.head
          Some(new CompositeRow(List(head, in)))      
        } else { None }
      }
      val results = parseq.filter(_.isDefined).map(_.get)
      // we can map the first one; the others must be inserted 
      // into the spool before we return it
      if(results.size > 0) {
        val sp = in *:: spool.tail
        if(results.size > 1) {
          Future(insertRowsIntoSpool(results.tail.seq, sp))
        } else {
          Future(sp)
        }
      } else {
        spool.tail
      }      
    }
  }
  
  /**
   * Implementors can override this method to make transformations.
   * Default is to perform no transformations.
   */
  @inline protected def transformIndexQuery(query: Q): Set[Q] = Set(query)
  
  /**
   * Return a list of references into the lookupSource from an index row.
   * Each one will create a new row in the result spool.
   */
  protected def getJoinablesFromIndexSource(index: Row): Array[M]
  
  override def request(query: Q): Future[Spool[Row]] = {
    val rowComp = rowCompWithOrdering(query.ordering.get.column, query.ordering.get.ordering)
    val queries = transformIndexQuery(query)
    val results = queries.par.map(mappedquery =>
      indexsource.request(mappedquery).flatMap(spool => spoolTransform(spool, mappedquery)))
    if(isOrdered(query)) {
      MergingResultSpool.mergeOrderedSpools(Seq(), results.seq.toSeq, rowComp, Array[Int]())
    } else {
      MergingResultSpool.mergeUnorderedResults(Seq(), results.seq.toSeq, Array[Int]())
    }
  }
  
  /**
   * implementors provide information whether their ordering is the same as 
   * or is a transformation of the one of the the original query.
   */
  @inline protected def isOrderedAccordingToOrignalOrdering(transformedQuery: Q, ordering: ColumnOrdering[_]): Boolean
  
  /**
   * As lookupSource is always ordered, ordering depends only on the original source
   * or on the transformed queries. If we have one unordered query everything is unordered.
   */ 
  @inline override def isOrdered(query: Q): Boolean = {
    query.getOrdering match {
      case Some(ordering) => transformIndexQuery(query).find(
          q => !(indexsource.isOrdered(q) && isOrderedAccordingToOrignalOrdering(q, ordering))).isEmpty
      case None => false
    }
  }
  
  /**
   * Splits up the source graph into two sub-graphs
   */
  override def getGraph: Graph[Source[DomainQuery, Spool[Row]], DiEdge] = (indexsource.getGraph + 
    DiEdge(indexsource.asInstanceOf[Source[DomainQuery, Spool[Row]]],
    this.asInstanceOf[Source[DomainQuery, Spool[Row]]])) ++ 
    (lookupSource.getGraph.asInstanceOf[Graph[Source[DomainQuery, Spool[Row]], DiEdge]] +
    DiEdge(lookupSource.asInstanceOf[Source[DomainQuery, Spool[Row]]],
    this.asInstanceOf[Source[DomainQuery, Spool[Row]]]))
    
  override def getDiscriminant: String = indexsource.getDiscriminant + lookupSource.getDiscriminant
  
  override def createCache: Cache[Nothing] = new NullCache
}
