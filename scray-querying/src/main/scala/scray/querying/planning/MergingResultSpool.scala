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

/**
 * used to combine query result sets 
 */
object MergingResultSpool {

//  def appendRowToSpool(row: Row, spool: Spool[Row]): Spool[Row] = {
//    
//  }
  
  /**
   * Returns the first result available. 
   */
//  def mergeUnorderedResults(seqs: Seq[Future[Seq[Row]]], spools: Seq[Future[Spool[Row]]]): Future[Spool[Row]] = {
//    if (!spools(0).get.isEmpty) {
//      // *:: for lazy/deferred tail
//      spools(0).get.head *:: mergeUnorderedResults(seqs, )
//    } else {
//      Spool.empty
//    }
//  }
//
//    
//    seqs(0).onSuccess( /* add all rows to spool */)
//    
//  }
//  
//

  
  /**
   * this is a simple merge step on sorted seqs and sorted spools
   */
  def mergeOrderedSpools(seqs: Seq[Future[Seq[Row]]], 
      spools: Seq[Future[Spool[Row]]],
      smaller: (Row, Row) => Boolean,
      indexes: Seq[Int]): Future[Spool[Row]] = Future.value {
    // find smallest head of spools
    val smallestOfSpools = spools.foldLeft[Option[Spool[Row]]](None){ (smallest, spool) =>
      val spoolFutureResult = Await.result(spool)  
      if(!spoolFutureResult.isEmpty) {
        smallest.map { smallvalue => 
          if(smaller(smallvalue.head, spoolFutureResult.head)) {
            smallvalue
          } else {
            spoolFutureResult
          }
        }.orElse(Some(spoolFutureResult))
      } else {
        smallest
      }
    }
    
    val smallestOfSeqs = findSmallestSeq(smaller, indexes, seqs, 0, None)
    
    // compare spool-result with seq-result
    smallestOfSeqs match {
      case Some((index, row, seqpos)) => smallestOfSpools.map { spool =>
        if(smaller(spool.head, row)) {
          // return spool-value as next value
          spool.head *:: mergeOrderedSpools(seqs, tailSpoolInSeq(spools, spool), smaller, indexes)
        } else {
          // return seq-value as next value
          val newIndexes = indexes.take(index) ++ Seq(seqpos + 1) ++ indexes.takeRight(indexes.size - index - 1)
          row *:: mergeOrderedSpools(seqs, spools, smaller, newIndexes)
        }
      }.getOrElse {
        // return seq-value as next value
        val newIndexes = indexes.take(index) ++ Seq(seqpos + 1) ++ indexes.takeRight(indexes.size - index - 1)        
        row *:: mergeOrderedSpools(seqs, spools, smaller, newIndexes)
      }
      case None => smallestOfSpools.map { spool =>
        // return spool-value as next value
        spool.head *:: mergeOrderedSpools(seqs, tailSpoolInSeq(spools, spool), smaller, indexes)
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
      acc: Option[(Int, Row, Int)]): Option[(Int, Row, Int)] = {
    if(indexesLocal.isEmpty) {
      acc
    } else {
      val seqLocal = Await.result(seqsLocal.head)
      val newAcc = if(indexesLocal.head < seqLocal.size) {
        acc.map { accu =>
          if(smaller(seqLocal.head, accu._2)) {
            (count, seqLocal.head, indexesLocal.head)
          } else {
            accu
          }
        }.orElse(Some((count, seqLocal.head, indexesLocal.head)))
      } else {
        acc
      }
      findSmallestSeq(smaller, indexesLocal.tail, seqsLocal.tail, count + 1, newAcc)
    }
  }  
}
