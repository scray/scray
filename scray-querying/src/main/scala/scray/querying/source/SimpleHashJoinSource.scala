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
import scray.querying.caching.Cache
import scray.querying.caching.NullCache
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.querying.description.EmptyRow

/**
 * This hash joined source provides a means to implement referential lookups (a simple join).
 * 
 * The query Q1 of "source" yields a spool which contains a column yielding a reference K.
 * The column value is then transformed into a reference R using typeMapper.
 * The query Q2 of "lookupSource" is then queried for R and the combined results are returned.
 * 
 * Default is a left outer join. 
 * To make this an inner join results must be filtered by setting isInner to true.
 */
class SimpleHashJoinSource[Q <: DomainQuery, K, R, V](
    source: LazySource[Q],
    sourceJoinColumn: Column,
    lookupSource: KeyValueSource[R, V],
    lookupSourceJoinColumns: List[Column],
    typeMapper: K => R = (k: K) => k.asInstanceOf[R],
    isInner: Boolean = false)
  extends LazySource[Q] with LazyLogging {

  /**
   * transforms the spool. May not skip too many elements because it is not tail recursive.
   */
  private def spoolTransform(spool: Spool[Row], query: Q): Spool[Row] = spool.map(in => {
    val inCol = in.getColumnValue[K](sourceJoinColumn)
    if(inCol.isDefined) {
      val keyValueSourceQuery = new SimpleKeyBasedQuery[R](
        typeMapper(inCol.get),
        lookupSourceJoinColumns,
        lookupSource.getColumns,
        query.querySpace,
        query.getQueryID)
      val seq = Await.result(lookupSource.request(keyValueSourceQuery))
      if(seq.size > 0) {
        val head = seq.head
        new CompositeRow(List(head, in))      
      } else { 
        handleNull(in)
      }
    } else { 
      handleNull(in) 
    }
  })
  
  @inline private def handleNull(row: Row) = if(isInner) {
    new EmptyRow
  } else {
    row
  }
  
  override def request(query: Q): Future[Spool[Row]] = {
    logger.debug(s"Joining in ${lookupSource.getDiscriminant} into ${source.getDiscriminant} for ${query.getQueryID}")
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
  
  override def getDiscriminant: String = source.getDiscriminant + lookupSource.getDiscriminant
  
  override def createCache: Cache[Nothing] = new NullCache
}
