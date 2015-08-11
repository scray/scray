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
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import com.twitter.concurrent.Spool.LazyCons
import com.twitter.concurrent.Spool.LazyCons
import scala.collection.mutable.ArrayBuffer
import scray.querying.queries.KeySetBasedQuery
import scray.common.serialization.KryoPoolSerialization
import java.io.FileOutputStream
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.Kryo
import scray.querying.description.RowColumn
import scray.querying.caching.serialization.RowColumnSerialization
import scray.querying.description.SimpleRow
import scray.querying.caching.serialization.ColumnSerialization
import scray.querying.caching.serialization.SimpleRowSerialization
import scala.util.hashing.MurmurHash3
import java.nio.ByteBuffer
import com.esotericsoftware.minlog.Log

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
    lookupkeymapper: Option[M => R] = None,
    sequencedmapper: Option[Int] = None)
  extends LazySource[Q] with LazyLogging {

  import SpoolUtils._
  
  /**
   * transforms the spool and queries the lookup source.
   */
  @inline protected def spoolTransform(spool: () => Spool[Row], query: Q): Future[Spool[Row]] = {
    @tailrec def insertRowsIntoSpool(rows: Array[Row], spoolElements: => Spool[Row]): Spool[Row] = {
      if(rows.isEmpty) {
        spoolElements
      } else {
        insertRowsIntoSpool(rows.tail, rows.head *:: Future.value(spoolElements))
      }
    }
    sequencedmapper.flatMap { seqSize =>
      lookupSource match {
        case pls: ParallelizedKeyValueSource[R, V] => 
          Some(Future.value(spool().mapBuffered(seqSize, bufferedseq => {
            val joinables = bufferedseq.flatMap(in => getJoinablesFromIndexSource(in))
            val setquery = new KeySetBasedQuery[R](
              joinables.map(lookupTuple => lookupkeymapper.map(_(lookupTuple)).getOrElse(lookupTuple.asInstanceOf[R])).toSet,
              lookupSourceTable,
              lookupSource.getColumns,
              query.querySpace,
              query.getQueryID)
            Await.result(pls.request(setquery))
          })))
        case _ => None
      }
    }.getOrElse {
      // if a reasonable limit (say 1000, we try to process in parallel)
      val pos = query.range.map { range => range.skip.getOrElse(0L) + range.limit.getOrElse(0L) }.getOrElse(Long.MaxValue)
      if(pos > 1000L) {
        spool().flatMap { in => 
          val joinables = getJoinablesFromIndexSource(in)          
          def joinableMapFunction: M => Option[Row] = { lookupTuple =>
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
          // execute this flatMap on a parallel collection if it exceeds a limit of 20
          if(joinables.size > 20) {
            val parseq: Array[Future[Option[Row]]] = joinables.map(futureJoinableMapFunction(query, in)(_))
            spoolRows(parseq)
          } else {
            // if there are less many entries we process it sequentially
            val parseq: Array[Option[Row]] = joinables.map { joinableMapFunction(_) }
            val results = parseq.filter(_.isDefined).map(_.get)
            if(results.size > 0) {
              // we can map the first one; the others must be inserted 
              // into the spool before we return it
              val sp = results(0) *:: Future.value(Spool.empty[Row])
              if(results.size > 1) {
                Future.value(insertRowsIntoSpool(results.tail, sp))
              } else {
                Future.value(sp)
              }
            } else {
              Future.value(in *:: Future.value(Spool.empty[Row]))
            }
          }
        }
      } else {
        spool().flatMap { in =>
          val joinables = getJoinablesFromIndexSource(in)
          val parseq: Array[Future[Option[Row]]] = joinables.map(futureJoinableMapFunction(query, in)(_))
          spoolRows(parseq)
        }
      }
    }
  }

  private def spoolRows(futures: Array[Future[Option[Row]]]): Future[Spool[Row]] = {
    if(futures.isEmpty) {
      Future.value(Spool.empty[Row])
    } else {
      futures.head.flatMap { optRow => 
        optRow.map(row => Future.value(row *:: spoolRows(futures.tail))).getOrElse(spoolRows(futures.tail))
      }
    }
  }

  private def futureJoinableMapFunction(query: Q, in: => Row): M => Future[Option[Row]] = { (lookupTuple) => 
    val keyValueSourceQuery = new KeyBasedQuery[R](
      lookupkeymapper.map(_(lookupTuple)).getOrElse(lookupTuple.asInstanceOf[R]),
      lookupSourceTable,
      lookupSource.getColumns,
      query.querySpace,
      query.getQueryID)
    val fseq = lookupSource.request(keyValueSourceQuery)
    fseq.map { seq =>  
      if(seq.size > 0) {
        val head = seq.head
        Some(new CompositeRow(List(head, in)))      
      } else { None }
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
    logger.debug(s"Joining in ${lookupSource.getDiscriminant} into ${indexsource.getDiscriminant} for ${query.getQueryID}")
    val queries = transformIndexQuery(query)
    val results = queries.par.map(mappedquery =>
      indexsource.request(mappedquery).flatMap(spool => spoolTransform(() => spool, mappedquery)))
    if(results.size > 1) {
      if(isOrdered(query)) {
        val rowComp = rowCompWithOrdering(query.ordering.get.column, query.ordering.get.ordering, query.ordering.get.descending)
        MergingResultSpool.mergeOrderedSpools(Seq(), results.seq.toSeq, rowComp, query.ordering.get.descending, Array[Int]())
      } else {
        MergingResultSpool.mergeUnorderedResults(Seq(), results.seq.toSeq, Array[Int]())
      }
    } else {
      results.head
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

object SpoolUtils {
  implicit class SpoolBufferExtender(val spool: Spool[Row]) {
    val buffer = new ArrayBuffer
    def mapBuffered(count: Int, f: Seq[Row] => Seq[Row]): Spool[Row] = {
      // auto-buffer next count elements; eager fetch
      @tailrec def fillSubBuffer(counter: Int, seq: Seq[Row], spool: => Spool[Row]): (Seq[Row], Spool[Row]) = {
        if(counter <= 0 || spool.isEmpty) {
          (seq, spool)
        } else {
          fillSubBuffer(counter - 1, seq :+ spool.head, Await.result(spool.tail))
        }
      }
      if (spool.isEmpty) {
        Spool.empty[Row]
      } else {
        // def _tail = spool.tail flatMap (tail => new SpoolBufferExtender(Await.result(tail)).mapBuffered(count, f)))
        fillSubBuffer(count, Seq(), spool) match {
          case (seq, reducedSpool) => rowSeqToSpool(f(seq)) ++ reducedSpool.mapBuffered(count, f)
        }
      }
    }
  }
  
  def rowSeqToSpool(seq: Seq[Row]): Spool[Row] = 
    if (!seq.isEmpty)
      seq.head *:: Future.value(rowSeqToSpool(seq.tail))
    else
      Spool.empty
}
