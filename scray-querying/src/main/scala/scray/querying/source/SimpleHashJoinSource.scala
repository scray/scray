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

import com.twitter.util.{Await, Future}
import com.twitter.concurrent.Spool
import scray.querying.queries.{DomainQuery, KeyBasedQuery}
import scray.querying.description.Column
import scray.querying.description.Row
import scray.querying.description.CompositeRow
import scalax.collection.immutable.Graph
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._
import scalax.collection.GraphEdge._
import scray.querying.queries.SimpleKeyBasedQuery

/**
 * This hash joined source provides a template for implementing hashed-joins. This is a relational lookup.
 * 
 * The query Q1 of "source" yields a spool which is composed of a key K and a reference R (i.e. (K, R)).
 * The query Q2 of "lookupSource" is then queried for K and the combined results are returned.
 * 
 * This is a left outer join. To make this an inner join results must be filtered.
 */
class SimpleHashJoinSource[Q <: DomainQuery, K, R, V](
    source: LazySource[Q],
    sourceJoinColumn: Column,
    lookupSource: KeyValueSource[R, V],
    lookupSourceJoinColumn: Column)
  extends LazySource[Q] {

  /**
   * transforms the spool. May not skip too many elements because it is not tail recursive.
   */
  private def spoolTransform(spool: Spool[Row], query: Q): Spool[Row] = {
    spool.map(in => {
      val inCol = in.getColumnValue(sourceJoinColumn)
      if(inCol.isDefined) {
        val keyValueSourceQuery = new SimpleKeyBasedQuery[R](
          inCol.get,
          lookupSourceJoinColumn,
          lookupSource.getColumns,
          query.querySpace,
          query.getQueryID)
        val seq = Await.result(lookupSource.request(keyValueSourceQuery))
        if(seq.size > 0) {
          val head = seq.head
          new CompositeRow(List(head, in))      
        } else { in }
      } else { in }
    })
  }
  
  override def request(query: Q): Future[Spool[Row]] = {
    source.request(query).map(spoolTransform(_, query))
  }
  
  override def getColumns: List[Column] = source.getColumns ++ lookupSource.getColumns
  
  // as lookupSource is always ordered, ordering depends only on the original source 
  override def isOrdered(query: Q): Boolean = source.isOrdered(query)
  
  override def getGraph: Graph[Source[DomainQuery, Spool[Row]], DiEdge] = (source.getGraph + 
    DiEdge(source.asInstanceOf[Source[DomainQuery, Spool[Row]]],
    this.asInstanceOf[Source[DomainQuery, Spool[Row]]])) ++ 
    (lookupSource.getGraph.asInstanceOf[Graph[Source[DomainQuery, Spool[Row]], DiEdge]] +
    DiEdge(lookupSource.asInstanceOf[Source[DomainQuery, Spool[Row]]],
    this.asInstanceOf[Source[DomainQuery, Spool[Row]]]))
}
