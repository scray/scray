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
import scray.querying.caching.{Cache, NullCache}
import scray.querying.description.{Column, EmptyRow, Row}
import scray.querying.queries.{DomainQuery, QueryInformation}

/**
 * dispenses empty rows, such that the results only contains rows which contain data
 */
class LazyEmptyRowDispenserSource[Q <: DomainQuery](val source: LazySource[Q], val qi: Option[QueryInformation] = None) extends LazySource[Q] with LazyLogging {

  override def request(query: Q): LazyDataFuture = {
    logger.debug(s"Filtering empty rows lazyly for ${query.getQueryID}")  
    source.request(query).flatMap { _.filter { row =>
        qi.map(_.resultItems.getAndIncrement)
        qi.map(queryinfo => queryinfo.pollingTime.set(System.currentTimeMillis()))
        !(row.isInstanceOf[EmptyRow] || row.isEmpty)
      }
    }
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
class EagerEmptyRowDispenserSource[Q <: DomainQuery, R](source: Source[Q, R], val qi: Option[QueryInformation] = None) extends EagerSource[Q] with LazyLogging {

  private def updateCounters(seq: => Seq[Row]): Unit = {
    val time = System.currentTimeMillis()
    qi.map(_.resultItems.getAndAdd(seq.size))
    qi.map(_.finished.set(time))
    qi.map(_.pollingTime.set(time))
  }

  override def request(query: Q): EagerDataFuture = {
    logger.debug(s"Filtering empty rows eagerly for ${query.getQueryID}")
    source.request(query).flatMap(_ match {
      case spool: Spool[_] =>
        spool.toSeq.asInstanceOf[EagerDataFuture] // collect
      case seq: Seq[_] =>
        Future(seq.asInstanceOf[Seq[Row]]) // do nothing
    }).map { seq =>
      updateCounters(seq)
      seq.filter(row => !(row.isInstanceOf[EmptyRow] || row.isEmpty))
    }
  }

  override def getColumns: List[Column] = source.getColumns

  override def isOrdered(query: Q): Boolean = source.isOrdered(query)

  override def getGraph: Graph[Source[DomainQuery, Seq[Row]], DiEdge] = source.asInstanceOf[Source[DomainQuery, Seq[Row]]].getGraph +
    DiEdge(source.asInstanceOf[Source[DomainQuery, Seq[Row]]],
    this.asInstanceOf[Source[DomainQuery, Seq[Row]]])

  override def getDiscriminant = "RowDispenser" + source.getDiscriminant

  override def createCache: Cache[Nothing] = new NullCache
}
