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
import com.typesafe.scalalogging.slf4j.LazyLogging
import scalax.collection.GraphEdge._
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._
import scalax.collection.immutable.Graph
import scray.querying.caching.Cache
import scray.querying.caching.NullCache
import scray.querying.description.Row
import scray.querying.queries.DomainQuery
import scala.collection.mutable.ArrayBuffer

/**
 * A source to lazily post-process data from a provided lazy source.
 */
abstract class LazyQueryMappingSource[Q <: DomainQuery](source: LazySource[Q]) 
  extends LazySource[Q] with LazyLogging {

  val optFunctions = new ArrayBuffer[(Row, Q) => Row]
  
  val optimized = source match {
    case mappingsource: LazyQueryMappingSource[Q] => 
      mappingsource.optimize(transformSpoolElement(_, _))
      true
    case _ => false
  }
  
  /**
   * do some optimization (less maps on spools) if the 
   * upstream source is of the same type.
   */
  def optimize(function: ((Row, Q) => Row)): Unit = {
    source match {
      case mappingsource: LazyQueryMappingSource[Q] => 
        mappingsource.optimize(function)
      case _ => 
        optFunctions += function
    }
  }
  
  /**
   * simple implementation starts a conversion process
   * subclasses implement transformSpoolElement
   */
  override def request(query: Q): LazyDataFuture = {
    logger.debug(s"Transforming elements lazyly for ${query.getQueryID}")
    init(query)
    if(optimized) {
      source.request(query)
    } else {
      source.request(query).map ( spool => spool.map { row => 
         val firstres = transformSpoolElement(row, query)
         optFunctions.foldLeft(firstres)((param, function) => function(param, query))
      })
    }
  }
  
  /**
   * implementors implement this method in order to lazily transform the Spool row-by-row.
   */
  def transformSpoolElement(element: Row, query: Q): Row

  def init(query: Q): Unit = {}
  
  override def isOrdered(query: Q): Boolean = source.isOrdered(query)
  
  override def getGraph: Graph[Source[DomainQuery, Spool[Row]], DiEdge] = source.getGraph + 
    DiEdge(source.asInstanceOf[Source[DomainQuery, Spool[Row]]],
    this.asInstanceOf[Source[DomainQuery, Spool[Row]]])
    
  override def createCache: Cache[Nothing] = new NullCache 
}

/**
 * A source to eagerly post-process data from a provided source.
 */
abstract class EagerCollectingQueryMappingSource[Q <: DomainQuery, R](source: Source[Q, R]) 
  extends EagerSource[Q] with LazyLogging {

  /**
   * simple implementation starts collecting and a conversion process
   * subclasses implement transformSpoolElement
   */
  override def request(query: Q): EagerDataFuture = {
    logger.debug(s"Transforming elements eagerly for ${query.getQueryID}")
    init(query)
    source.request(query).flatMap(_ match {
      case spool: Spool[_] => spool.toSeq.asInstanceOf[Future[Seq[Row]]] //collect
      case seq: Seq[_] => Future(seq.asInstanceOf[Seq[Row]]) // do nothing
    }).map(transformSeq(_, query))
  }
  
  /**
   * implementors implement this method in order to map the resultset on the whole
   * Default is to call transformSeqElement on each element of the Seq.
   */
  def transformSeq(element: Seq[Row], query: Q): Seq[Row] = {
    element.map(transformSeqElement(_, query))
  }
  
  /**
   * implementors override this to achieve transformation of a single row
   */
  def transformSeqElement(element: Row, query: Q): Row
  
  def init(query: Q): Unit = {}
  
  override def isOrdered(query: Q): Boolean = source.isOrdered(query)
  
  override def getGraph: Graph[Source[DomainQuery, Seq[Row]], DiEdge] = source.asInstanceOf[Source[DomainQuery, Seq[Row]]].getGraph + 
    DiEdge(source.asInstanceOf[Source[DomainQuery, Seq[Row]]],
    this.asInstanceOf[Source[DomainQuery, Seq[Row]]])
    
  override def createCache: Cache[Nothing] = new NullCache 
}
