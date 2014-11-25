package scray.querying.caching

import scray.querying.queries.DomainQuery
import scray.querying.source.LazyDataFuture

trait Cache[V] {

  /**
   * retrieve data from the cache
   */
  def retrieve(query: DomainQuery): Option[V]
  
  /**
   * do maintnance on this cache, e.g. remove old rows
   */
  def maintnance: Unit
  
  /**
   * closes the cache and frees up it's resources
   */
  def close: Unit
}