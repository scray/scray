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
import scala.collection.mutable.ArrayBuffer
import com.twitter.util.Await
import scala.annotation.tailrec
import scray.querying.description.QueryRange
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
 * used to combine query result sets or skip and limit
 */
object MergingResultSpool extends LazyLogging {

  /**
   * Returns the first result available. 
   */
  def mergeUnorderedResults(seqs: Seq[Future[Seq[Row]]],
      spools: Seq[Future[Spool[Row]]],
      indexes: Seq[Int]): Future[Spool[Row]] = Future.value {
    def submitSpool(spoolFuture: Future[Spool[Row]]): Spool[Row] = {
      val spool = Await.result(spoolFuture)
      spool.head *:: mergeUnorderedResults(seqs, tailSpoolInSeq(spools, spool), indexes)      
    }
    def submitSeq(info: Option[(Int, Row, Int)]): Spool[Row] = info match {
      case Some((index, row, seqpos)) => 
        val newIndexes = indexes.take(index) ++ Seq(seqpos + 1) ++ indexes.takeRight(indexes.size - index - 1)        
        row *:: mergeUnorderedResults(seqs, spools, newIndexes)
      case None => Spool.empty
    }
    @tailrec def submitFirstValidSeqOrEmpty(locSeqs: Seq[Future[Seq[Row]]], locIndexes: Seq[Int], count: Int): Spool[Row] = {
      if(locSeqs.size <= 0) {
        Spool.empty
      } else {
        val result = Await.result(locSeqs.head)
        if(locIndexes.head < result.size) {
          submitSeq(Some((count, result(locIndexes.head), locIndexes.head)))
        } else {
          submitFirstValidSeqOrEmpty(locSeqs.tail, locIndexes, count + 1)
        }
      }
    }
    @tailrec def submitFirstValidSpoolOrEmpty(locSpools: Seq[Future[Spool[Row]]]): Spool[Row] = {
      if(locSpools.size <= 0) {
        submitFirstValidSeqOrEmpty(seqs, indexes, 0)
      } else {
        val result = Await.result(locSpools.head)
        if(!result.isEmpty) {
          submitSpool(locSpools.head)
        } else {
          submitFirstValidSpoolOrEmpty(locSpools.tail)
        }
      }
    }
    val seqAvailableResults = findAvailableSeq(indexes, seqs, 0, None)
    if(seqAvailableResults.isEmpty) {
      val spoolAvailableResults = spools.find { spool =>
        val spoolFuturePoll = spool.poll 
        spoolFuturePoll match {
          case Some(pollTry) => !pollTry.get.isEmpty
          case None => false
        }
      }
      spoolAvailableResults match {
        case Some(spoolFuture) => submitSpool(spoolFuture)
        case None => submitFirstValidSpoolOrEmpty(spools)
      }
    } else {
      submitSeq(seqAvailableResults)
    }
  }

  
  /**
   * this is a simple merge step on sorted seqs and sorted spools
   */
  def mergeOrderedSpools(seqs: Seq[Future[Seq[Row]]], 
      spools: Seq[Future[Spool[Row]]],
      smaller: (Row, Row) => Boolean,
      descending: Boolean,
      indexes: Seq[Int]): Future[Spool[Row]] = Future.value {
    // find smallest head of spools
    val smallestOfSpools = spools.foldLeft[Option[Spool[Row]]](None){ (smallest, spool) =>
      val spoolFutureResult = Await.result(spool)  
      if(!spoolFutureResult.isEmpty) {
        smallest.map { smallvalue => 
          if(descending ^ smaller(smallvalue.head, spoolFutureResult.head)) {
            smallvalue
          } else {
            spoolFutureResult
          }
        }.orElse(Some(spoolFutureResult))
      } else {
        smallest
      }
    }
    
    val smallestOfSeqs = findSmallestSeq(smaller, indexes, seqs, 0, None, descending)
    
    // compare spool-result with seq-result
    smallestOfSeqs match {
      case Some((index, row, seqpos)) => smallestOfSpools.map { spool =>
        if(descending ^ smaller(spool.head, row)) {
          // return spool-value as next value
          spool.head *:: mergeOrderedSpools(seqs, tailSpoolInSeq(spools, spool), smaller, descending, indexes)
        } else {
          // return seq-value as next value
          val newIndexes = indexes.take(index) ++ Seq(seqpos + 1) ++ indexes.takeRight(indexes.size - index - 1)
          row *:: mergeOrderedSpools(seqs, spools, smaller, descending, newIndexes)
        }
      }.getOrElse {
        // return seq-value as next value
        val newIndexes = indexes.take(index) ++ Seq(seqpos + 1) ++ indexes.takeRight(indexes.size - index - 1)        
        row *:: mergeOrderedSpools(seqs, spools, smaller, descending, newIndexes)
      }
      case None => smallestOfSpools.map { spool =>
        // return spool-value as next value
        spool.head *:: mergeOrderedSpools(seqs, tailSpoolInSeq(spools, spool), smaller, descending, indexes)
      }.getOrElse(Spool.empty)
    }
  }
  
  /**
   * call tail on a spool in a seq and return it as a new seq
   */
  private def tailSpoolInSeq(spools: Seq[Future[Spool[Row]]], spool: Spool[Row]): Seq[Future[Spool[Row]]] = spools.map { fut => 
    val locSpool = Await.result(fut)
    if(locSpool == spool) {
      spool.tail
    } else {
      fut
    }
  }
  
  /**
   * find smallest seq which index is smaller than its end, and returns an 
   * index into Seqs and a Row and an index within
   */ 
  @tailrec private def findSmallestSeq(smaller: (Row, Row) => Boolean,
      indexesLocal: Seq[Int],
      seqsLocal: Seq[Future[Seq[Row]]],
      count: Int,
      acc: Option[(Int, Row, Int)],
      descending: Boolean): Option[(Int, Row, Int)] = {
    if(indexesLocal.isEmpty) {
      acc
    } else {
      val seqLocal = Await.result(seqsLocal.head)
      val newAcc = if(indexesLocal.head < seqLocal.size) {
        acc.map { accu =>
          if(descending ^ smaller(seqLocal(indexesLocal.head), accu._2)) {
            (count, seqLocal(indexesLocal.head), indexesLocal.head)
          } else {
            accu
          }
        }.orElse(Some((count, seqLocal(indexesLocal.head), indexesLocal.head)))
      } else {
        acc
      }
      findSmallestSeq(smaller, indexesLocal.tail, seqsLocal.tail, count + 1, newAcc, descending)
    }
  }
  
  /**
   * find smallest seq which index is smaller than its end, and returns an 
   * index into Seqs and a Row and an index within
   */ 
  @tailrec private def findAvailableSeq(indexesLocal: Seq[Int],
      seqsLocal: Seq[Future[Seq[Row]]],
      count: Int,
      acc: Option[(Int, Row, Int)]): Option[(Int, Row, Int)] = {
    if(indexesLocal.isEmpty) {
      acc
    } else {
      val res = seqsLocal.head.poll match {
        case Some(seqLocalTry) => seqLocalTry.map(seqLocal => if(indexesLocal.head < seqLocal.size) {
          // we have a result, return that thing
          Some((count, seqLocal(indexesLocal.head), indexesLocal.head))
        } else {
          None
        }).get
        case None => None 
      }
      if(res.isDefined) {
        res
      } else {
        findAvailableSeq(indexesLocal.tail, seqsLocal.tail, count + 1, acc)
      }
    }
  }
  
  /**
   * seek and limit a given spool 
   */
  def seekingLimitingSpoolTransformer(spool: Spool[Row], range: Option[QueryRange]): Spool[Row] = {
    def slspooltcount(spool: Spool[Row], count: Long, skip: Long): Spool[Row] = {
      if(spool.isEmpty) {
        // end of data has been reached, return the empty spool
        spool
      } else {
        if(count < skip) {
          // there is still data we need to skip
          slspooltcount(Await.result(spool.tail), count + 1L, skip)
        } else {
          if(range.get.limit.isEmpty) {
            // skipping ended and no limit, return rest of data
            spool
          } else {
            if(count < skip + range.get.limit.get) {
              // re-insert data
              if(count == skip + range.get.limit.get - 1L) {
                spool.head *:: Future.value(Spool.empty[Row])
              } else {
                spool.head *:: Future.value(slspooltcount(Await.result(spool.tail), count + 1L, skip))
              }
            } else {
              // limit has been reached, return the empty spool
              Spool.empty[Row]
            }
          }
        }
      }
    }
    if(range.isDefined && (range.get.skip.isDefined || range.get.limit.isDefined)) {
      val skip = range.get.skip.getOrElse(0L)
      slspooltcount(spool, 0L, skip)
    } else {
      spool
    }
  }
}
