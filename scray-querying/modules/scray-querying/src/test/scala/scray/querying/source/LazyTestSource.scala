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
import com.twitter.concurrent.Spool
import scray.querying.description.Row
import com.twitter.util.Future
import scray.querying.caching.Cache
import scalax.collection.immutable.Graph
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._
import scalax.collection.GraphEdge._
import scray.querying.description.Column
import scray.querying.caching.NullCache
import sun.reflect.generics.reflectiveObjects.NotImplementedException
import com.typesafe.scalalogging.LazyLogging

class LazyTestSource(spool: Spool[Row], ordered: Boolean = true) extends LazySource[DomainQuery] with LazyLogging {
  override def request(query: DomainQuery): LazyDataFuture = Future.value(spool)
  override def isLazy: Boolean = true
  override def getColumns: Set[Column] = spool.headOption.map { x => x.getColumns.toSet }.getOrElse( throw EmptySpoolException )
  override def isOrdered(query: DomainQuery): Boolean = ordered
  override def getGraph: Graph[Source[DomainQuery, Spool[Row]], DiEdge] = Graph.from(List(this), List())
  override def getDiscriminant: String = this.getClass.getName + spool.headOption.map { x => x.toString }.getOrElse("Empty") 
  override def createCache: Cache[_] = new NullCache
  
  object EmptySpoolException extends Exception("Spool may not be empty!")
}

class EagerTestSource(source: Seq[Row], ordered: Boolean = true) extends EagerSource[DomainQuery] with LazyLogging {
  override def request(query: DomainQuery): EagerDataFuture = Future.value(source)
  override def isLazy: Boolean = true
  override def getColumns: Set[Column] = source.headOption.map { x => x.getColumns.toSet }.getOrElse( throw EmptySpoolException )
  override def isOrdered(query: DomainQuery): Boolean = ordered
  override def getGraph: Graph[Source[DomainQuery, Seq[Row]], DiEdge] = source.asInstanceOf[Source[DomainQuery, Seq[Row]]].getGraph +
    DiEdge(source.asInstanceOf[Source[DomainQuery, Seq[Row]]],
    this.asInstanceOf[Source[DomainQuery, Seq[Row]]])
  override def getDiscriminant: String = this.getClass.getName + source.headOption.map { x => x.toString }.getOrElse("Empty") 
  override def createCache: Cache[_] = new NullCache
  
  object EmptySpoolException extends Exception("Spool may not be empty!")
}