package scray.querying.caching

import scray.querying.queries.DomainQuery

class QueryableCache[Q <: DomainQuery, T] extends Cache[T] {
  override def retrieve(query: DomainQuery): Option[T] = None
  override def maintnance: Unit = {}
  override def close: Unit = {}

}