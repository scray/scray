//package scray.querying.source.costs
//
//import scray.querying.queries.DomainQuery
//import scray.querying.source.SimpleHashJoinSource
//import scray.querying.source.KeyValueSource
//
//class  KeyValueSourceCosts [Q <: DomainQuery, K, R, V](source: KeyValueSource[K, V], factory: QueryCostFunctionFactory) extends CostFunction[Q] {
//  def apply(query: Q): QueryCosts = {
//    val sourceCosts = source.getCosts(query)(factory)
//    val amount = sourceCosts.estimatedAmount + sourceCosts.estimatedCardinality 
//    QueryCosts(amount, sourceCosts.estimatedCardinality)
//  }
//}