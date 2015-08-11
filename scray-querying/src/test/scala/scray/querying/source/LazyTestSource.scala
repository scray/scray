package scray.querying.source

import scray.querying.queries.DomainQuery
import com.typesafe.scalalogging.slf4j.LazyLogging
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

class LazyTestSource(spool: Spool[Row], ordered: Boolean = true) extends LazySource[DomainQuery] with LazyLogging {
  override def request(query: DomainQuery): LazyDataFuture = Future.value(spool)
  override def isLazy: Boolean = true
  override def getColumns: List[Column] = spool.headOption.map { x => x.getColumns }.getOrElse( throw EmptySpoolException )
  override def isOrdered(query: DomainQuery): Boolean = ordered
  override def getGraph: Graph[Source[DomainQuery, Spool[Row]], DiEdge] = Graph.from(List(this), List())
  override def getDiscriminant: String = this.getClass.getName + spool.headOption.map { x => x.toString }.getOrElse("Empty") 
  override def createCache: Cache[_] = new NullCache
  
  object EmptySpoolException extends Exception("Spool may not be empty!")
}