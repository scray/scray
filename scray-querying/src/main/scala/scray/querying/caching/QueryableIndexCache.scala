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
package scray.querying.caching

import scray.querying.queries.DomainQuery
import scray.querying.description.Column
import org.mapdb.DBMaker
import com.twitter.concurrent.Spool
import java.util.UUID
import scray.querying.description.internal.Domain
import scray.querying.description.Row
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock
import scray.querying.source.QueryableSource
import scala.annotation.tailrec
import com.twitter.util.Future
import scray.querying.description.internal.SingleValueDomain
import scray.querying.description.internal.RangeValueDomain
import scray.querying.caching.serialization.QueryableCacheSerializer
import org.mapdb.SerializerBase
import scray.querying.description.internal.RangeValueDomain
import scray.querying.description.internal.SingleValueDomain
import scala.collection.mutable.ListBuffer
import scray.querying.description.internal.RangeValueDomain
import scray.querying.description.internal.Bound

/**
 * To increase performance, this Cache assumes that it is placed directly in front 
 * of the QueryableSource. This allows to assume that there are no predicated in use
 * that limit the result set further than the one of the index.
 * 
 * Internal rule to prevent dead-locks: 
 * * first grab parent locks
 * * then grab child locks
 */ /**
class QueryableOrderedIndexCache[Q <: DomainQuery](col: Column,
    primaryColumns: List[Column],
    queryableSource: QueryableSource[Q, Row],
    storeSize: Double = 1.0d,
    approximateBlockSize: Short = 1000,
    maxBlockEntries: Int = 10000000) extends Cache[Row] {

  // needs off heap data cache
  val db = DBMaker.newMemoryDirectDB().transactionDisable().make
  val cache = db.createHashMap("cache").counterEnable().expireStoreSize(storeSize).
    keySerializer(new SerializerBase).valueSerializer(new QueryableCacheSerializer()).make[UUID, ArrayBuffer[Row]]
  
  abstract class Tree(val col: String, val lock: ReentrantReadWriteLock) {
    private var lastAccess: Long = System.currentTimeMillis()
    def getLastAccess: Long = lastAccess
    def accessed: Unit = lastAccess = System.currentTimeMillis()
    def getChild(childcol: String): Option[Tree]
  }
  case class TreeNode[V](col: String, domain: Domain[V], children: ArrayBuffer[Tree], override val lock: ReentrantReadWriteLock) 
    extends Tree(col, lock) {
    override def getChild(childcol: String): Option[Tree] = children.find(_.col == childcol)
  }
  case class TreeLeaf[V](col: String, var domain: Domain[V], cacheReference: UUID, parent: TreeNode[V], var count: Int,
      override val lock: ReentrantReadWriteLock)(implicit ordering: Ordering[V]) extends Tree(col, lock) {
    var complete: Boolean = false
    override def getChild(childcol: String): Option[Tree] = None
    /**
     * re-calculate the domain of this leaf by looking at rows contained
     */
    def defineDomainFromRows(): Option[Domain[V]] = {
      lock.readLock.lock
      try {
        val buf = cache.get(cacheReference)
        val bufsize = buf.size
        if(bufsize > 0 && !buf(0).isInstanceOf[CacheServeMarkerRow]) {
          val lower = buf(0).getColumnValue[V](domain.column)
          val upper = if(buf(bufsize - 1).isInstanceOf[CacheServeMarkerRow]) {
            buf(bufsize - 2).getColumnValue[V](domain.column)
          } else {
            buf(bufsize - 1).getColumnValue[V](domain.column)
          }
          if(lower.get == upper.get) {
            Some(SingleValueDomain[V](domain.column, lower.get))
          } else {
            Some(RangeValueDomain[V](domain.column,
                Some(new Bound[V](true, lower.get)(ordering)),
                Some(new Bound[V](true, upper.get)(ordering))))
          }
        } else {
          None
        }
      } finally {
        lock.readLock.unlock
      }
    }

    val rowOrdering: (Row, Row) => Boolean = (row1, row2) => {
      if(row1.isInstanceOf[CacheServeMarkerRow]) {
        false
      } else {
        if(row2.isInstanceOf[CacheServeMarkerRow]) {
          true
        } else {
          val val1 = row1.getColumnValue[V](domain.column)
          val val2 = row2.getColumnValue[V](domain.column)
          if(val1.isEmpty) {
            false
          } else {
            if(val2.isEmpty) {
              true
            } else {
              ordering.compare(val1.get, val2.get) < 0
            }
          }
        }
      }
    } 
    
    /**
     * warning: uses write locks!
     */
    def tryJoinDomains() {      
      def compareLowerBoundOfDomains(domain1: Domain[V], domain2: Domain[V]): Boolean = domain1.asInstanceOf[RangeValueDomain[V]].lowerBound match {
        case Some(lower1) => domain2.asInstanceOf[RangeValueDomain[V]].lowerBound match {
          case Some(lower2) => domain1.asInstanceOf[RangeValueDomain[V]].ordering.compare(lower1.value, lower2.value) <= 0
          case None => false
        }
        case None => true
      }
      // check if this domain is contained in some other domain
      parent.lock.writeLock().lock()
      try {
        val scheduledRemoves = ListBuffer[(Int, Boolean)]()
        val selfnumber = parent.children.foldLeft(0) { (count, child) => if(child eq this) {
            count
          } else {
            count + 1
          }
        }
        parent.children.foldLeft(0) { (count, child) =>
          if(child ne this) {
            child match {
              case leaf: TreeLeaf[V] => leaf.domain match {
                case single: SingleValueDomain[V] => domain match {
                  case thissingle: SingleValueDomain[V] => if(thissingle.equiv.equiv(single.value, thissingle.value)) {
                    scheduledRemoves += ((count, false))
                  }
                  case thisrange: RangeValueDomain[V] => if(thisrange.valueIsInBounds(single.value)) {
                    scheduledRemoves += ((count, false))
                  }
                }
                case range: RangeValueDomain[V] => domain match {
                  case thissingle: SingleValueDomain[V] => if(range.valueIsInBounds(thissingle.value)) {
                	scheduledRemoves += ((selfnumber, false))
                  }
                  case thisrange: RangeValueDomain[V] => if(thisrange.union(range).isDefined) { 
                    scheduledRemoves += ((count, true))
                  }
                }
              }
              case _ =>
            }
          }
          count + 1
        }
        // order nodes descending, to make sure removes are not changing indexes of entries
        val removes = scheduledRemoves.sortWith((part1, part2) => part1._1 > part2._1)
        var selfremove = false
        removes.foreach { remove =>
          val child = parent.children(remove._1).asInstanceOf[TreeLeaf[V]]
          child.lock.writeLock.lock
          try {
            lock.writeLock.lock
            try {
              if(remove._2) {
                val (buf1, buf2) = compareLowerBoundOfDomains(domain, child.domain) match { 
                  case true => (cache.get(cacheReference), cache.get(child.cacheReference))
                  case false => (cache.get(child, cacheReference), cache.get(cacheReference))
                }
                // we must check that the merge will not produce a cache with more entries than allowed
                if(child.count + count < 1000) {
                  if(buf1.size > 0) {
                    if(buf1(buf1.size - 1).isInstanceOf[CacheServeMarkerRow]) {
                      buf1.remove(buf1.size - 1)
                    }
                  }
                  buf1 ++= buf2
                  buf1.sortWith(rowOrdering)
                  cache.put(cacheReference, buf1)
                  count = buf1.size
                  defineDomainFromRows() match {
                    case Some(locdomain) => domain = locdomain
                    case None => selfremove = true
                  }
                  cache.remove(child.cacheReference)
                  parent.children.remove(remove._1)
                } else {
                  // create 2 buffers that are comprised of 1000 + x elements
                  val (buf1, buf2) = compareLowerBoundOfDomains(domain, child.domain) match { 
                    case true => (cache.get(cacheReference), cache.get(child.cacheReference))
                    case false => (cache.get(child, cacheReference), cache.get(cacheReference))
                  }
                  if(buf1.size > 0) {
                    val index = buf1(buf1.size - 1).isInstanceOf[CacheServeMarkerRow] match {
                      case true => buf1.size - 2 // the last entry is a marker, then we overwrite it
                      case false => buf1.size - 1
                    }
                    buf1.insertAll(index, buf2)
                    buf1.sortWith(rowOrdering)
                    // if the cache is not completed, we produce an incomplete cache
              
                    // if both caches are completed 
                    
                    // the newly created domains
//                    } else {
//                    
//                    }
                  }
                }
              } else {
                cache.remove(child.cacheReference)
                parent.children.remove(remove._1)
              }
            } finally {
              lock.writeLock.unlock
            }
          } finally {
            child.lock.writeLock.unlock
          }
          if(selfremove) {
            lock.writeLock.lock
            try {
              cache.remove(cacheReference)
              parent.children.remove(selfnumber)
            } finally {
              lock.writeLock.unlock
            }
          }
        }
      } finally {
        parent.lock.writeLock.unlock
      }
    }
  }
  
  // needs on heap query tree
  var queryTree: Option[Tree] = None 

  /**
   * different result types
   */
  object RowResult extends Enumeration {
    type RowResult = Value
    val unknown, // result has not come to an end, similar but not explicitly a cacheInclomplete notifier
    cacheExhausted, // cache block is exhausted, i.e. no more elements will follow from this block
    cacheIncomplete = Value // this cache block does not contain all data, must query from last row on
  }
  
  /**
   * creates a result spool from a cached block of rows
   */
  @inline private def insertCacheBlock[V](tree: TreeLeaf[V], block: UUID, domain: Domain[_]): (Spool[Row], RowResult.RowResult) = {
    var row = RowResult.unknown
    @tailrec def spoolBlock(seq: ArrayBuffer[Row], count: Int, seqsize: Int): Spool[Row] = {
      if(count >= seqsize || seq(count).isInstanceOf[CompleteCacheServeMarkerRow]) {
        row = RowResult.cacheExhausted
        Spool.empty
      } else {
        if(seq(count).isInstanceOf[IncompleteCacheServeMarkerRow]) {
          row = RowResult.cacheIncomplete
          Spool.empty
        } else {
          val value = seq(count).getColumnValue(domain.column)
          val include = if(value.isDefined) {
            domain match {
              case single: SingleValueDomain[_] => single.equiv.equiv(single.value, value.get)
              case range: RangeValueDomain[_] => range.valueIsInBounds(value.get)
              case _ => true
            }
          } else {
            true
          }
          if(!include) {
            spoolBlock(seq, count -1, seqsize)
          } else {
            seq(count) *:: Future.value(spoolBlock(seq, count -1, seqsize))
          }
        }
      }
    }
    tree.lock.readLock.lock
    try {
      val seq = cache.get(block)
      (spoolBlock(seq, 0, seq.size), row)
    } finally {
      tree.lock.readLock.unlock
    }
  }
  
  private def navigateToLeaf(sortedDomains: List[Domain[_]]) = {
    
  }
  
  override def retrieve(query: DomainQuery): Option[Spool[Row]] = {
    None // Some(insertCacheBlock(tree, block, domain))
  }
  
  override def maintnance: Unit = {
    cache.clear()
    queryTree = None
  }
  
  override def close: Unit = maintnance
  
  def canRetrieveFromThisCache(query: Q, col: Column, primaryColumns: List[Column],
      value: Spool[Row], complete: Boolean): Boolean
  
  /**
   * caches the spool while returning it (i.e. write-through-cache) 
   */
  def putSpool(query: Q): Spool[Row] = {
    val domains = query.getWhereAST.collect { 
      case domain: Domain[_] if domain.column == col => domain
      case primary: Domain[_] if primaryColumns.contains(primary) => primary
    }
    val domainGroup = domains.partition(_.column.columnName == col.columnName)
    val sortedDomains = domainGroup._2.sortWith(columnOrdering)
//    if(domainGroup._1.isEmpty) {
//      // if the column is not contained in the query, we cache everything
//      value.map(cacheRow(_, col))
//    } else {
//      // if domains are contained in each other, we do not cache, but can perform a retrieve
//      // if domains overlap, we can increase the result-set by joining the caches
//    }
      
      // if the column is contained in the query, we can check a number of cases:
      
      // we cache in chunks of 1000 rows, to circumvent heap overload
    Spool.empty
  }
  
  private def cacheRow(row: Row, column: Column, domain: Option[Domain[_]] = None): Row = {
    // if the current block is contained in the cache, retrieve cache 
    // if the current row is not contained in the cache, then cache
    row
  }
  
  val columnOrdering: (Domain[_], Domain[_]) => Boolean = 
    (domain1, domain2) => domain1.column.columnName >= domain2.column.columnName 
} */