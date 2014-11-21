package scray.querying.caching

import scray.querying.queries.DomainQuery

class NullCache extends Cache[Nothing] {
  override def retrieve(query: DomainQuery): Option[Nothing] = None
  override def maintnance: Unit = {}
  override def close: Unit = {}
}