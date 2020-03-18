package scray.querying.source

import scray.querying.queries.DomainQuery

package object costs {
  type CostFunction[Q <: DomainQuery] = Q => QueryCosts
}