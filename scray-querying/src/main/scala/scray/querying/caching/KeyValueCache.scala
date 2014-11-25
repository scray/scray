package scray.querying.caching

import scray.querying.queries.DomainQuery
import scray.querying.source.LazyDataFuture
import org.mapdb._
import scray.querying.queries.KeyBasedQuery
import com.twitter.concurrent.Spool
import scray.querying.description.internal.WrongQueryTypeForCacheException

class KeyValueCache[K, V](
    val sourceDiscriminant: String,
    val cachesizegb: Double = 1.0D,
    val numberentries: Option[Int] = None) extends Cache[V] {

  val db = DBMaker.newMemoryDirectDB().transactionDisable().asyncWriteEnable().make
  val cache = db.createHashMap("cache").counterEnable().make[K, V]
  
  /**
   * retrieve one row and end up with CompleteCacheServeMarkerRow
   * if successful
   */
  override def retrieve(query: DomainQuery): Option[V] = query match {
    case keyquery: KeyBasedQuery[K] => Option(cache.get(keyquery.key))
    case _ => throw new WrongQueryTypeForCacheException(query, sourceDiscriminant)
  }

  def put(query: DomainQuery, value: V) = query match {
    case keyquery: KeyBasedQuery[K] => cache.put(keyquery.key, value)
    case _ => throw new WrongQueryTypeForCacheException(query, sourceDiscriminant)
  }
  
  override def maintnance: Unit = {
    // expiring old columns can be handled automatically by MapDB
    // only job is to make sure that it doesn't overflow
  }
  
  override def close: Unit = {
    cache.close
  }
}