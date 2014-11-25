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

import scray.querying.queries.DomainQuery
import scray.querying.description.{Column, EmptyRow}
import scalax.collection.immutable.Graph
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._
import scalax.collection.GraphEdge._
import com.twitter.concurrent.Spool
import scray.querying.description.Row
import com.twitter.util.Future
import scray.querying.caching.Cache
import scray.querying.caching.NullCache

/**
 * dispenses empty rows, such that the results only contains rows which contain data
 */
class LazyEmptyRowDispenserSource[Q <: DomainQuery](val source: LazySource[Q]) extends LazySource[Q] {

  override def request(query: Q): LazyDataFuture = {
    source.request(query).flatMap(_.filter(row => !(row.isInstanceOf[EmptyRow] || row.isEmpty)))
  } 

  override def getColumns: List[Column] = source.getColumns
  
  override def isOrdered(query: Q): Boolean = source.isOrdered(query)
  
  override def getGraph: Graph[Source[DomainQuery, Spool[Row]], DiEdge] = source.getGraph + 
    DiEdge(source.asInstanceOf[Source[DomainQuery, Spool[Row]]],
    this.asInstanceOf[Source[DomainQuery, Spool[Row]]])

  override def getDiscriminant = "RowDispenser" + source.getDiscriminant
  
  override def createCache: Cache[Nothing] = new NullCache
}

/**
 * dispense all empty rows in the requested source
 */
class EagerEmptyRowDispenserSource[Q <: DomainQuery, R](source: Source[Q, R]) extends EagerSource[Q] {
  
  override def request(query: Q): EagerDataFuture = {
    source.request(query).flatMap(_ match {
      case spool: Spool[_] => spool.toSeq.asInstanceOf[EagerDataFuture] //collect
      case seq: Seq[_] => Future(seq.asInstanceOf[Seq[Row]]) // do nothing
    }).map(_.filter(row => !(row.isInstanceOf[EmptyRow] || row.isEmpty)))
  }
  
  override def getColumns: List[Column] = source.getColumns
  
  override def isOrdered(query: Q): Boolean = source.isOrdered(query)
    
  override def getGraph: Graph[Source[DomainQuery, Seq[Row]], DiEdge] = source.asInstanceOf[Source[DomainQuery, Seq[Row]]].getGraph + 
    DiEdge(source.asInstanceOf[Source[DomainQuery, Seq[Row]]],
    this.asInstanceOf[Source[DomainQuery, Seq[Row]]])
    
  override def getDiscriminant = "RowDispenser" + source.getDiscriminant
  
  override def createCache: Cache[Nothing] = new NullCache
}
