package scray.querying.source.costs

import scray.querying.queries.DomainQuery
import scray.querying.source.Source


trait QueryCostFunctionFactory {
  def getCostFunction[Q <: DomainQuery, T](source: Source[Q, T]): CostFunction[Q]
}