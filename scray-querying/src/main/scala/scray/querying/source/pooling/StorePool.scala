package scray.querying.source.pooling

import java.util.TimeZone
import scray.querying.description.{Column, Row}
import com.twitter.storehaus.QueryableStore
import com.twitter.storehaus.ReadableStore
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.HashMap
import com.twitter.util.Time
import com.twitter.util.Duration
import com.twitter.util.JavaTimer
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
 * A simple pooling mechanism for Storehaus stores
 */
class StorePool[S](val storefunc: Long => S, version: Option[Long], val checkFree: Option[S => Boolean] = None, val maxStores: Int = 7) extends LazyLogging {
  
  val queryLock = new ReentrantLock
  val storeLock = queryLock // new ReentrantLock
  
  val stores = new ArrayBuffer[S] 
  val queryMappings = new HashMap[String, Int]()
  
  val rand = new Random
  
  def getStore(queryId: String): Option[S] = {
    version.map { ver =>
      queryLock.lock()
      try {
        queryMappings.get(queryId).map(stores(_)).getOrElse {
          val newStore = getFreeNewOrRandomStore(ver)
          queryMappings += ((queryId, newStore._2))
          newStore._1
        }
      } finally {
        queryLock.unlock()
      }
    }
  }
  
  def getFreeNewOrRandomStore(ver: Long): (S, Int) = {
    storeLock.lock()
    try {
      val res = stores.foldLeft[(Int, Option[S])]((-1, None)) { (num, fstore) => 
        if(num._2.isEmpty) {
          checkFree.map {  f => if(f(fstore)) {
              (num._1 + 1, Some(fstore))
            } else {
              (num._1 + 1, None)
            }
          }.getOrElse {
            (num._1 + 1, None)
          }
        } else {
          num
        }
      }
      if(res._2.isEmpty) {
        if(stores.size <= (maxStores - 1)) {
          val store = storefunc(ver)
          stores += store
          (store, stores.size - 1)
        } else {
          // return random store
          val num = rand.nextInt(stores.size)
          (stores(num), num)
        }
      } else {
        (res._2.get, res._1)
      }
    } finally {
      storeLock.unlock()
    }    
  }
  
  def close: Unit = {
    storeLock.lock()
    try {
      stores.foreach { store => 
        checkFree.map { check => 
          if(check(store)) {
            store.asInstanceOf[ReadableStore[_,_]].close(Duration.fromSeconds(120).fromNow)
          } else {
            val timer = new JavaTimer(true) {
              var count = 0
            }
            timer.schedule(Duration.fromSeconds(10)) {
              if(check(store)) {
                timer.count += 1
                if(timer.count == 3) {
                  store.asInstanceOf[ReadableStore[_,_]].close(Duration.fromSeconds(120).fromNow)
                  timer.stop()              
                }
              } else {
                timer.count = 0
              }
            }
          }
        }
      }
      stores.clear
      queryMappings.clear
    } finally {
      storeLock.unlock()
    }
  }
}
