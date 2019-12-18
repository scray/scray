package scray.querying.source.costs

import scray.querying.queries.DomainQuery
import scray.querying.source.SimpleHashJoinSource

class LinearSimpleHashJoinCosts[Q <: DomainQuery, K <: DomainQuery, V](source: SimpleHashJoinSource[Q, K, V], factory: QueryCostFunctionFactory) extends CostFunction[Q] {
  def apply(query: Q): QueryCosts = {
    val sourceCosts = source.source.getCosts(query)(factory)
    val lookupCosts = source.lookupSource.getCosts(source.getKeyedQuery(query, Set()))(factory)
    val amount = sourceCosts.estimatedAmount + sourceCosts.estimatedCardinality * lookupCosts.estimatedAmount
    QueryCosts(amount, sourceCosts.estimatedCardinality)
  }
  def getCosts(query: Q): QueryCosts = apply(query)
}